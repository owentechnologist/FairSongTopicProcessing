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
 * This program showcases the use of the Jedis Streams client library
 * possible args:
 * --publishnew   true/false should this program execution write new song entries?
 * --eventcount 100  int how many events to process (including new entries or audited entries)
 * --converttofair  true/false should this program execution populate the FairTopic with songs?
 * --convertthreadnum  int how many threads to spin up that do the converting to Hashes?
 * --searchwritethreadnum in how many threads to spin up that search and write new entries to the FairTopic?
 * --convertcount   int how many entries should each search thread process before exiting?
 * --audittopics  true/false should thie program execution create topK entries showing entry counts by singer?
 *
 * Example:  mvn compile exec:java -Dexec.cleanupDaemonThreads=false -Dexec.args="--host redis-FIXME.c309.FIXME.cloud.redisFIXME.com --port 12144 --password FIXME <required-args>"

 * Example with all required args publishing only new Song entries:
 ```
 mvn compile exec:java -Dexec.cleanupDaemonThreads=false -Dexec.args="--host redis-FIXME.com --port 0000 --password FIXME --converttofair false --audittopics false --publishnew true --eventcount 1500"
 ```
 * Example with all required args converting Song entries to FairEntries:
 ```
 mvn compile exec:java -Dexec.cleanupDaemonThreads=false -Dexec.args="--host redis-FIXME.com --port 0000 --password FIXME --audittopics false --publishnew false --converttofair true --convertthreadnum 10 --searchwritethreadnum 1 --convertcount 1500"
 ```
 * Example with all required args producing topK results from INBOUND and FAIR Topics:
 ```
 mvn compile exec:java -Dexec.cleanupDaemonThreads=false -Dexec.args="--host redis-FIXME.com --port 0000 --password FIXME --publishnew false --converttofair false --audittopics true --eventcount 1500"
 ```

 * It is expected that the Search module is enabled and the following index has been created...
 * Execute the following from redis-cli / redisInsight to create the necessary index:
 ```
 FT.CREATE idx_songs prefix 1 song SCHEMA singer TAG SORTABLE album TAG SORTABLE isTombstoned TAG isQueued TAG isThrottled TAG TimeOfArrival NUMERIC SORTABLE
 ```
 *
 * There will always be at most 15 singers,
 * 100 albums per singer,
 * and at most 1,000 songs per album.
 */
public class Main {

    static String INBOUND_TOPIC_NAME = "InboundSongsTopic";
    static String READY_FOR_FAIR_PROCESSING_TOPIC_NAME = "FairProcessingTopic";
    static String CF_INBOUND_TOPIC_KEY_NAME = "CF:" + INBOUND_TOPIC_NAME;
    static String CF_PROCESSING_TARGET_KEY_NAME = "CF:ProcessingTargetFilter";
    static String SEARCH_IDX = "idx_songs";
    static ArrayList<String> argList = null;

    /**
     * In this version, Messages will be ack'd when they are dupes or
     * when they are successfully added to the Search Index and processed
     * TODO: the deletion is not implemented yet...
     * <deletion-not-implemented>
     * To delete the message processed by a consumer add:
     * --delmessage true
     * </deletion-not-implemented>
     *
     * @param args
     * @throws Throwable The actual Exceptions likely to be thrown include:
     *                   com.redis.streams.exception.InvalidMessageException;
     *                   com.redis.streams.exception.InvalidTopicException;
     *                   com.redis.streams.exception.ProducerTimeoutException;
     *                   com.redis.streams.exception.TopicNotFoundException;
     */
    public static void main(String[] args) throws Throwable {

        argList = new ArrayList<>(Arrays.asList(args));
        System.out.println("args have length of :" + args.length);
        JedisPooled connection = JedisPooledHelper.getJedisPooledFromArgs(args);
        System.out.println("BEGIN TEST (RESPONSE TO PING) -->   " + connection.ping());
        long startTime = System.currentTimeMillis();

        boolean publishNew = Boolean.parseBoolean(getValueForArg("--publishnew"));
        if(publishNew) {
            publishNewSongs(connection);
        }

        boolean convertToFair = Boolean.parseBoolean(getValueForArg("--converttofair"));
        if(convertToFair) {
            int howManyConvertEntryThreads = Integer.parseInt(getValueForArg("--convertthreadnum"));
            for (int threads = 0; threads < howManyConvertEntryThreads; threads++) {
                //this class has the ability to see if there are unread entries
                //it will process all the unread entries...
                InboundSongTopicProcessorThread fsptt = new InboundSongTopicProcessorThread().
                        setPooledJedis(connection).
                        setInboundSongTopicName(INBOUND_TOPIC_NAME).
                        setInboundEntryDedupKeyName(CF_INBOUND_TOPIC_KEY_NAME);
                new Thread(fsptt).start();
            }

            int howManySearchers = Integer.parseInt(getValueForArg("--searchwritethreadnum"));
            for(int searchers=0;searchers< howManySearchers; searchers++){
                FairTopicEntryFromSearchCreatorThread ftefsct = new FairTopicEntryFromSearchCreatorThread().
                        setJedisPooled(connection).
                        setNumberOfEntriesToConsumePerThread(Integer.parseInt(getValueForArg("--convertcount"))).
                        setReadyForFairProcessingTopicName(READY_FOR_FAIR_PROCESSING_TOPIC_NAME).
                        setDedupProcessingTargetKeyName(CF_PROCESSING_TARGET_KEY_NAME).
                        setSearchIndexName(SEARCH_IDX);
                new Thread(ftefsct).start();
            }
        }

        boolean auditTopics = Boolean.parseBoolean(getValueForArg("--audittopics"));
        if(auditTopics) {
            topicTopKAuditorOnly(connection);
        }

    }

    /**
     * Use this method to Consume and Process the events in the Fair Topic
     * @param args
     * @param connection
     * @throws Throwable
     */
    public static void consumeFromFairTopic(JedisPooled connection)throws Throwable{
        //being that this is a demo...
        // as long as we use the auditing method - we don't really need to process anything
    }

    /**
     * Use this method to publish new songs to the INBOUND Topic:
     * @param args
     * @param connection
     * @throws Throwable
     */
    public static void publishNewSongs(JedisPooled connection)throws Throwable{
        long retentionTimeSeconds = 86400 * 8;
        long maxStreamLength = 2500;
        long streamCycleSeconds = 86400; // Create a new stream after one day, regardless of the current stream's length
        SerialTopicConfig config = new SerialTopicConfig(
                INBOUND_TOPIC_NAME,
                retentionTimeSeconds,
                maxStreamLength,
                streamCycleSeconds,
                SerialTopicConfig.TTLFuzzMode.RANDOM);
        TopicManager manager = TopicManager.createTopic(connection, config);
        int howManySongEvents = Integer.parseInt(getValueForArg("--eventcount"));
        TopicProducer producer = new TopicProducer(connection,INBOUND_TOPIC_NAME);
        NewSongEventWriter nsew = new NewSongEventWriter().setTopicProducer(producer).setHowManySongEventsToWrite(howManySongEvents).setSleepMillisBetweenWrites(100);
        new Thread(nsew).start();
    }

    /**
     * Use this method to audit the Entries in both of the Topics
     * INBOUND and FAIR
     * @param args
     */
    public static void topicTopKAuditorOnly(JedisPooled connection){
        int howManySongEvents=Integer.parseInt(getValueForArg("--eventcount"));
        //This next section uses the TopicConsumerThread to log TopK counts of message Attributes seen:
        //track singers added to FAIR TOPIC_NAME:
        TopicEntriesTopKAuditorThread topicRFFPConsumerThread = new TopicEntriesTopKAuditorThread().
                setTopicName(READY_FOR_FAIR_PROCESSING_TOPIC_NAME).
                setConsumerGroupName("RFFP_TOPK_CONSUMER_GROUP").
                setJedisPooledConnection(connection).
                setConsumerInstanceName("1").
                setNumberOfMessagesToProcess(howManySongEvents).
                setAttributeNameToTrack("singer").
                setTopKSize(15);
        new Thread(topicRFFPConsumerThread).start();

        //Track singers added to INBOUND_TOPIC_NAME

        TopicEntriesTopKAuditorThread topicInboundConsumerThread = new TopicEntriesTopKAuditorThread().
                setTopicName(INBOUND_TOPIC_NAME).
                setConsumerGroupName("INBOUND_TOPK_CONSUMER_GROUP").
                setJedisPooledConnection(connection).
                setConsumerInstanceName("1").
                setNumberOfMessagesToProcess(howManySongEvents).
                setAttributeNameToTrack("singer").
                setTopKSize(15);
        new Thread(topicInboundConsumerThread).start();
    }

    /**
     * Use this method to retrieve the String version of any runtime Arg
     * Be sure to convert it to the type you need in your code after it is retrieved
     * @param arg
     * @return
     */
    public static String getValueForArg(String arg){
        String response = null;
        if (argList.contains(arg)) {
            int argIndex = argList.indexOf(arg);
            response = argList.get(argIndex + 1);
        }else{
            throw new RuntimeException("\nOOPS!\nYou need to provide the following expected argument "+arg);
        }
        return response;
    }
}