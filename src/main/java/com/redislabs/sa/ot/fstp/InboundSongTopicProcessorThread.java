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
public class InboundSongTopicProcessorThread extends Thread{

    JedisPooled connection = null;
    String INBOUND_TOPIC_NAME=null;
    String DEDUP_INBOUND_TOPIC_KEY_NAME=null;
    static volatile int consumerIDSCounterForThreads=0;
    int consumerIDSuffixForThread=0;

    public InboundSongTopicProcessorThread setInboundEntryDedupKeyName(String name){
        this.DEDUP_INBOUND_TOPIC_KEY_NAME=name;
        return this;
    }

    public InboundSongTopicProcessorThread setPooledJedis(JedisPooled conn){
        this.connection=conn;
        return this;
    }

    public InboundSongTopicProcessorThread setInboundSongTopicName(String topicName){
        this.INBOUND_TOPIC_NAME=topicName;
        return this;
    }

    public void run(){
        if((null==connection)||
                (null==INBOUND_TOPIC_NAME)||
                (null==DEDUP_INBOUND_TOPIC_KEY_NAME)){
            throw new RuntimeException("\n\t---> MISSING PROPERTIES - you must set all properties before starting this Thread.");
        }
        try{
            consumerIDSCounterForThreads++;
            this.consumerIDSuffixForThread=consumerIDSCounterForThreads;
            System.out.println("InboundSongTopicProcessorThread Started...");
            long lag = 1;
            while(lag>0){
                ConsumerGroup group = convertInboundToSearchableHashes(consumerIDSuffixForThread);
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
     * Every 20th song or so will be set to throttled - just to prove that has an impact
     * @param args
     * @param connection
     * @throws Throwable
     */
    ConsumerGroup convertInboundToSearchableHashes(int consumerIDSuffixForThread)throws Throwable {
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
}