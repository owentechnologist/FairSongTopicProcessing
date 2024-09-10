package com.redislabs.sa.ot.fstp;

import redis.clients.jedis.*;
import com.redis.streams.*;
import com.redis.streams.command.serial.*;
import com.redis.streams.exception.InvalidMessageException;
import com.redis.streams.exception.InvalidTopicException;
import com.redis.streams.exception.ProducerTimeoutException;
import com.redis.streams.exception.TopicNotFoundException;
import java.util.Map;
import com.redislabs.sa.ot.util.*;

/**
 * This Thread executes as many times as the numberOfEntriesToConsume which is derived from:
 * --convertcount <somenumber>
 */
public class FairTopicEntryFromSearchCreatorThread extends Thread{

    String READY_FOR_FAIR_PROCESSING_TOPIC_NAME = null;
    String DEDUP_PROCESSING_TARGET_KEY_NAME = null;
    String searchIndexName = null;
    JedisPooled connection = null;
    int numberOfEntriesToConsume = 0;


    public FairTopicEntryFromSearchCreatorThread setSearchIndexName(String searchIndexName){
        this.searchIndexName=searchIndexName;
        return this;
    }

    public FairTopicEntryFromSearchCreatorThread setDedupProcessingTargetKeyName(String dedupProcessingTargetKeyName){
        this.DEDUP_PROCESSING_TARGET_KEY_NAME=dedupProcessingTargetKeyName;
        return this;
    }

    public FairTopicEntryFromSearchCreatorThread setReadyForFairProcessingTopicName(String readyForFairProcessingTopicName){
        this.READY_FOR_FAIR_PROCESSING_TOPIC_NAME=readyForFairProcessingTopicName;
        return this;
    }

    public FairTopicEntryFromSearchCreatorThread setNumberOfEntriesToConsumePerThread(int numberOfEntriesToConsumePerThread){
        this.numberOfEntriesToConsume=numberOfEntriesToConsumePerThread;
        return this;
    }

    public FairTopicEntryFromSearchCreatorThread setJedisPooled(JedisPooled connection){
        this.connection=connection;
        return this;
    }


    /**
     * new Thread(FairTopicEntryFromSearchCreatorThread).start() drives this method
     */
    public void run(){
        if((null==connection)||(null==searchIndexName)||
                (null==READY_FOR_FAIR_PROCESSING_TOPIC_NAME)||
                (null==DEDUP_PROCESSING_TARGET_KEY_NAME)){
            throw new RuntimeException("\n\t---> MISSING PROPERTIES - you must set all properties before starting this Thread.");
        }
        try {
            searchAndAddToFairProcessingTopic();
        }catch(Throwable t){t.printStackTrace();}
    }

    /**
     * Where the work is done to search and find matching entries that then get written
     * To the FairProcessingTopic
     * @throws InvalidTopicException
     * @throws TopicNotFoundException
     * @throws InvalidMessageException
     * @throws ProducerTimeoutException
     */
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
                try{
                    Thread.sleep(10);
                }catch(Throwable t){}
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