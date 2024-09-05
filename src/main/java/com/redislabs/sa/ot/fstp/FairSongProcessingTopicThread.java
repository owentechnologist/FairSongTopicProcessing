package com.redislabs.sa.ot.fstp;

import redis.clients.jedis.*;

import java.util.ArrayList;
import java.util.Arrays;

import com.redis.streams.*;
import com.redis.streams.command.serial.*;
import com.redis.streams.exception.InvalidMessageException;
import com.redis.streams.exception.InvalidTopicException;
import com.redis.streams.exception.ProducerTimeoutException;
import com.redis.streams.exception.TopicNotFoundException;
import java.util.Map;
import com.redislabs.sa.ot.util.*;

/**
 * This class enables multi-threaded processing of songs written to a Topic
 * It consumes the song entries and judges if they should be delayed due to the
 * same Singer's song having just come through the system within the past 2 seconds
 * or not added to the FairProCessingTopic at all because they are duplicates
 * Some songs are randomly Throttled so that they are not added at all until some
 * Other process searches for them and changes their isThrottled flag to false
 */
public class FairSongProcessingTopicThread extends Thread{

    JedisPooled connection = null;
    int numberOfEntriesToConsume=10;
    String INBOUND_TOPIC_NAME=null;
    String READY_FOR_FAIR_PROCESSING_TOPIC_NAME=null;
    String DEDUP_INBOUND_TOPIC_KEY_NAME=null;
    String DEDUP_PROCESSING_TARGET_KEY_NAME=null;
    String searchIndexName = null;
    static volatile int consumerIDSCounterForThreads=0;
    int consumerIDSuffixForThread=0;

    public FairSongProcessingTopicThread setInboundEntryDedupKeyName(String name){
        this.DEDUP_INBOUND_TOPIC_KEY_NAME=name;
        return this;
    }
    public FairSongProcessingTopicThread setFairProcessingEntryDedupKeyName(String name){
        this.DEDUP_PROCESSING_TARGET_KEY_NAME=name;
        return this;
    }
    public FairSongProcessingTopicThread setSearchIndexName(String name){
        this.searchIndexName=name;
        return this;
    }

    public FairSongProcessingTopicThread setNumberOfEntriesToConsume(int howMany){
        this.numberOfEntriesToConsume=howMany;
        return this;
    }
    public FairSongProcessingTopicThread setPooledJedis(JedisPooled conn){
        this.connection=conn;
        return this;
    }
    public FairSongProcessingTopicThread setInboundSongTopicName(String topicName){
        this.INBOUND_TOPIC_NAME=topicName;
        return this;
    }
    public FairSongProcessingTopicThread setFairProcessingSongTopicName(String topicName){
        this.READY_FOR_FAIR_PROCESSING_TOPIC_NAME=topicName;
        return this;
    }

    public void run(){
        if((null==connection)||(null==searchIndexName)||
                (null==INBOUND_TOPIC_NAME)||(null==READY_FOR_FAIR_PROCESSING_TOPIC_NAME)||
                (null==DEDUP_INBOUND_TOPIC_KEY_NAME)||(null==DEDUP_PROCESSING_TARGET_KEY_NAME)){
            throw new RuntimeException("\n\t---> MISSING PROPERTIES - you must set all properties before starting this Thread.");
        }
        try{
            consumerIDSCounterForThreads++;
            this.consumerIDSuffixForThread=consumerIDSCounterForThreads;
            System.out.println("FairSongProcessingTopicThread Started...");
            if(consumerIDSuffixForThread%2==1) { //mixing up the behavior of this class
                searchAndAddToFairProcessingTopic();
            }
            long lag = 1;
            while(lag>0){
                ConsumerGroup group = convertInboundToFair(consumerIDSuffixForThread);
                lag=((ConsumerGroupBase)group).getCurrentLagForThisInstanceTopicAndGroup();
            }
        }catch(Throwable t){t.printStackTrace();}
    }

    /**
     * Use this method to convert the Inbound Songs to the Fair Topic
     * This involves deduping the entries using a CuckooFilter and
     * writing an enriched version of the entry as hash and
     * querying the search index for the next fair entry
     * (sorted by date ASC and !queued and grouped by album)
     * Every 20th song will be set to throttled - just to prove that has an impact
     * @param args
     * @param connection
     * @throws Throwable
     */
    ConsumerGroup convertInboundToFair(int consumerIDSuffixForThread)throws Throwable {
        //FIXME: consumed entries are not being Acknowledged!!!
        String consumerGroupName = "groupA";
        ConsumerGroup consumer = new ConsumerGroup(connection, INBOUND_TOPIC_NAME, consumerGroupName);
        //for (int x = 0; x < numberOfEntriesToConsume; x++) {
            TopicEntry consumedMessage = consumer.consume("songEventFairProcessor:"+consumerIDSuffixForThread);
            System.out.println("FairSongProcessingTopicThread.convertInboundToFair() DEBUG: Topic gave me: "+consumedMessage);
            if(!(null==consumedMessage)) {
                consumedMessage.getMessage().forEach(
                (key, value) -> System.out.println(key + ":" + value)
                );
                String singer = consumedMessage.getMessage().get("singer");
                String dedupValue = singer;
                dedupValue += "_" + consumedMessage.getMessage().get("album");
                dedupValue += "_" + consumedMessage.getMessage().get("song");

                boolean isDuplicate = true; // same song on same album same singer do not process
                boolean shouldDelay = false; // same singer as seen in past 2 seconds so throttle

                isDuplicate=SlidingWindowHelper.itemExistsWithinTimeWindow("Z:"+DEDUP_INBOUND_TOPIC_KEY_NAME,dedupValue,connection,300);
                shouldDelay=SlidingWindowHelper.itemExistsWithinTimeWindow("Z:SINGERS_DELAYED",singer,connection,2);
                //What do we do if we shouldThrottle?
                //A: do not add it to the FairProcessingTopic now, but instead, wait 2 seconds
                if(shouldDelay){
                    //sleep for 2 seconds giving other songs a chance to be added to the
                    //FairTopic by other Threads
                    Thread.sleep(2000);
                }
                System.out.println("isDuplicate ==" + isDuplicate);
                //If not duplicate then isDuplicate == false:
                if (!isDuplicate) {
                    //write the message attributes to a Hash to be indexed by Search
                    //use the eventID from the stream as the UID in the hash keyname:
                    String uid = consumedMessage.getId().toString();
                    System.out.println("EntryID = " + uid);
                    String album = consumedMessage.getMessage().get("album");
                    String song = consumedMessage.getMessage().get("song");
                    String lyrics = consumedMessage.getMessage().get("lyrics");
                    String releaseDate = consumedMessage.getMessage().get("releaseDate");
                    String timeOfArrival = uid.split("-")[0];
                    String isTombstoned = "false";
                    String isQueued = "false";
                    String isThrottled = "false";
                    if (System.nanoTime() % 50 == 0) { //rare event
                        isThrottled = "true";
                    }
                    Map map = Map.of("singer", singer, "album", album, "song", song, "releaseDate",
                            releaseDate, "lyrics", lyrics, "TimeOfArrival", timeOfArrival,
                            "isTombstoned", isTombstoned, "isQueued", isQueued, "isThrottled", isThrottled);
                    connection.hset("song:" + uid, map);
                }
            }// end of if(!null==consumedMessage)
            try{
                if(!(null==consumedMessage)) {
                    // To acknowledge a message, create an AckMessage:
                    AckMessage ack = new AckMessage(consumedMessage);
                    boolean success = consumer.acknowledge(ack);
                    System.out.println("Inbound song to be processed Message Acknowledged... " + ack);
                }
            }catch(Throwable ttt){ttt.printStackTrace();}
        //}//end of convertTargetCount loop
        return consumer;
    }

    void searchAndAddToFairProcessingTopic() throws InvalidTopicException,
            TopicNotFoundException,InvalidMessageException,ProducerTimeoutException{
        /**
         * Next section focuses on moving searched/sorted/filtered entries into the
         * FairTopic...
         * Search criteria includes: isQueued false
         * As part of the processing this method sets isQueued to true
         */
        long retentionTimeSeconds = 86400 * 8;
        long maxStreamLength = 2500;
        long streamCycleSeconds = 86400; // Create a new stream after one day, regardless of the current stream's length
        SerialTopicConfig config2 = new SerialTopicConfig(
                READY_FOR_FAIR_PROCESSING_TOPIC_NAME,
                retentionTimeSeconds,
                maxStreamLength,
                streamCycleSeconds,
                SerialTopicConfig.TTLFuzzMode.RANDOM);
        TopicManager fairTopicManager = TopicManager.createTopic(connection, config2);
        TopicProducer fairProducer = new TopicProducer(connection, READY_FOR_FAIR_PROCESSING_TOPIC_NAME);
        //use search to find the fair SongEvent to Process Next
        SearchHelper searcher = new SearchHelper();
        for (int x = 0; x < numberOfEntriesToConsume; x++) {
            boolean entityExists = true;
            String hashKeyNameToProcessNext = "";
            int retryCount = 10; //retry 10 times to allow time for new entries to arrive
            while ((entityExists) && (retryCount > 0)) {
                hashKeyNameToProcessNext = searcher.performSearchGetResultHashKey(connection, searchIndexName, 1);
                //dedup the song_album_singer using SortedSet with 5 min window (300 seconds:
                entityExists = SlidingWindowHelper.itemExistsWithinTimeWindow("Z:" + DEDUP_PROCESSING_TARGET_KEY_NAME, hashKeyNameToProcessNext, connection, 300);
                System.out.println("Hmmm...  entityExists == " + entityExists + " keyname == " + hashKeyNameToProcessNext);
                retryCount--;
            }
            //Process the Song Event (not necessarily the latest one just received)
            //second topic is populated with fairly distributed events across all albums and singers
            String fairSingerName = null;
            if(null!=hashKeyNameToProcessNext) {
                fairSingerName = connection.hget(hashKeyNameToProcessNext, "singer");
            }
            if (null != fairSingerName) {
                //Dedup the song_album_singer using CuckooFilter
                if (!connection.exists(DEDUP_PROCESSING_TARGET_KEY_NAME)) {
                    connection.cfReserve(DEDUP_PROCESSING_TARGET_KEY_NAME, 10000000);
                }
                boolean isNewTargetKey = connection.cfAddNx(DEDUP_PROCESSING_TARGET_KEY_NAME, hashKeyNameToProcessNext);
                if(isNewTargetKey) {
                    fairProducer.produce(Map.of("keyName", hashKeyNameToProcessNext, "singer", fairSingerName));
                    connection.hset(hashKeyNameToProcessNext, "isQueued", "true");
                    //TimeSeriesEventLogger uses JedisPooledHelper to init JedisPooled connection when created:
                    //To make logger more robust you can assign startUpArgs to the instance (not done in this example)
                    TimeSeriesEventLogger logger = new TimeSeriesEventLogger().setSharedLabel("fair_events").
                            setCustomLabel(fairSingerName).
                            setTSKeyNameForMyLog("TS:" + fairSingerName).
                            initTS();
                    logger.addEventToMyTSKey(1.0d);
                }
            }
        }//end of for loop: numberOfEntriesToConsume
    }
}