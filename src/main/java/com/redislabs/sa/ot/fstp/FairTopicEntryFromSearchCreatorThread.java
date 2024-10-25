package com.redislabs.sa.ot.fstp;

import redis.clients.jedis.*;
import java.util.logging.*;
import com.redis.streams.*;
import com.redis.streams.command.serial.*;
import com.redis.streams.exception.InvalidMessageException;
import com.redis.streams.exception.InvalidTopicException;
import com.redis.streams.exception.ProducerTimeoutException;
import com.redis.streams.exception.TopicNotFoundException;
import java.util.Map;
import java.util.ArrayList;
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
    int sleepmillis = 10;
    static Logger logger = Logger.getLogger("com.redislabs.sa.ot.fstp.FairTopicEntryFromSearchCreatorThread");


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

    public FairTopicEntryFromSearchCreatorThread setSleepDefault(int sleepDefault){
        this.sleepmillis=sleepDefault;
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
            throw new RuntimeException("\n\tFairTopicEntryFromSearchCreatorThread---> MISSING PROPERTIES - you must set all properties before starting this Thread.");
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
        // at some point this should be replaced with endless loop
        // as we shouldn't have a limit on numberOfEntriesToConsume:
        for (int x = 0; x < numberOfEntriesToConsume; x++) {
            boolean entityExists = true;
            int randChoice = 0;
            int resultLength = 0;
            int numberOfPossibleResults=30; // this should be tweaked for optimal result size
            String singularKeyNameToProcessNext = "";
            ArrayList<String> hashKeyNamesToProcessNext = null;
            int retryCount = 10; //retry 10 times to allow time for new entries to arrive
            while ((entityExists) && (retryCount > 0)) {
                try{
                    Thread.sleep(sleepmillis);
                }catch(Throwable t){}
                hashKeyNamesToProcessNext = searcher.performSearchGetResultHashKey(connection, searchIndexName, numberOfPossibleResults);
                resultLength = hashKeyNamesToProcessNext.size();
                logger.fine("Debug FairsearchCreator... hashKeyNamesToProcessNext.size();== "+resultLength);
                //dedup the song_album_singer using SortedSet with 5 min window (300 seconds:
                //randomly choose from the results to limit collisions with other threads:
                if(resultLength>0){
                    randChoice = (int)(System.nanoTime()%resultLength);
                    singularKeyNameToProcessNext = hashKeyNamesToProcessNext.get(randChoice);
                    entityExists = SlidingWindowHelper.itemExistsWithinTimeWindow("Z:" + DEDUP_PROCESSING_TARGET_KEY_NAME, singularKeyNameToProcessNext, connection, 300);

                    logger.fine("Hmmm...  entityExists == " + entityExists + " keyname == " + singularKeyNameToProcessNext);
                }
                retryCount--;
            }
            //Process the Song Event
            //second topic should be populated with fairly distributed events across all albums and singers
            String fairSingerName = null;
            if(null!=singularKeyNameToProcessNext) {
                fairSingerName = connection.hget(singularKeyNameToProcessNext, "singer");
            }
            if (null != fairSingerName) {
                //Dedup the song_album_singer using CuckooFilter
                if (!connection.exists(DEDUP_PROCESSING_TARGET_KEY_NAME)) {
                    connection.cfReserve(DEDUP_PROCESSING_TARGET_KEY_NAME, 10000000);
                }
                boolean isNewTargetKey = connection.cfAddNx(DEDUP_PROCESSING_TARGET_KEY_NAME, singularKeyNameToProcessNext);
                if(isNewTargetKey) {
                    try{
                        fairProducer.produce(Map.of("keyName", singularKeyNameToProcessNext, "singer", fairSingerName));
                        connection.hset(singularKeyNameToProcessNext, "isQueued", "true");

                        //TimeSeriesEventLogger uses JedisPooledHelper to init JedisPooled connection when created:
                        //To make logger more robust you can assign startUpArgs to the instance (not done in this example)
                        TimeSeriesEventLogger tsLogger = new TimeSeriesEventLogger().setSharedLabel("fair_events").
                                setCustomLabel(fairSingerName).
                                setTSKeyNameForMyLog("TS:" + fairSingerName).
                                initTS();
                        tsLogger.addEventToMyTSKey(1.0d);
                    }catch(java.lang.NumberFormatException nfe){}
                }
            }
        }//end of for loop: numberOfEntriesToConsume
    }

}