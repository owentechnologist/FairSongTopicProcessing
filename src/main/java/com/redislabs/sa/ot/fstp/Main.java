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
 * --eventcount 100
 * Example:  mvn compile exec:java -Dexec.cleanupDaemonThreads=false -Dexec.args="--host redis-FIXME.c309.FIXME.cloud.redisFIXME.com --port 12144 --password FIXME <required-args>"

 * Example with all required args publishing only new Song entries:
mvn compile exec:java -Dexec.cleanupDaemonThreads=false -Dexec.args="--host redis-12144.c309.us-east-2-1.ec2.cloud.redislabs.com --port 12144 --password WqedzS2orEF4Dh0baBeaRqo16DrYYxzI --eventcount 1500 --publishnew true --converttofair false --convertcount 0 --audittopics false"

 * Example with all required args converting Song entries to FairEntries:

mvn compile exec:java -Dexec.cleanupDaemonThreads=false -Dexec.args="--host redis-12144.c309.us-east-2-1.ec2.cloud.redislabs.com --port 12144 --password WqedzS2orEF4Dh0baBeaRqo16DrYYxzI --eventcount 1500 --publishnew false --converttofair true --convertcount 10 --audittopics false"

 * It is expected that the Search module is enabled and this index has been created...
 * Execute the following from redis-cli / redisInsight if not:
 * FT.CREATE idx_songs prefix 1 song SCHEMA singer TAG SORTABLE album TAG SORTABLE isTombstoned TAG isQueued TAG isThrottled TAG TimeOfArrival NUMERIC SORTABLE
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
    static long cfReserveSize = 10000000;//roughly 16 mb / key
    static ArrayList<String> argList = null;

    /**
     * In this version, Messages will be ack'd when they are dupes or
     * when they are successfully added to the Search Index and processed
     * To delete the message processed by a consumer add:
     * --delmessage true
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
            convertInboundToFair(connection);
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
    public static void convertInboundToFair(JedisPooled connection)throws Throwable {
        int convertTargetCount = Integer.parseInt(getValueForArg("--convertcount"));
        String consumerGroupName = "groupA";
        ConsumerGroup consumer = new ConsumerGroup(connection, INBOUND_TOPIC_NAME, consumerGroupName);
        for (int x = 0; x < convertTargetCount; x++) {
            TopicEntry consumedMessage = consumer.consume("songEventFairProcessor");
            if(!(null==consumedMessage)) {
                consumedMessage.getMessage().forEach(
                        (key, value) -> System.out.println(key + ":" + value)
                );
                String dedupValue = consumedMessage.getMessage().get("singer");
                dedupValue += "_" + consumedMessage.getMessage().get("album");
                dedupValue += "_" + consumedMessage.getMessage().get("song");

                boolean isDuplicate = true;
                boolean shouldAck = false;

                /*
                //Dedup the selected keyName using CuckooFilter

                if (!connection.exists(CF_INBOUND_TOPIC_KEY_NAME)) {
                    connection.cfReserve(CF_INBOUND_TOPIC_KEY_NAME, cfReserveSize);
                }
                isNew = connection.cfAddNx(CF_INBOUND_TOPIC_KEY_NAME, dedupValue);
                */

                isDuplicate=SlidingWindowHelper.itemExistsWithinTimeWindow("Z:"+CF_INBOUND_TOPIC_KEY_NAME,dedupValue,connection,300);

                System.out.println("isDuplicate ==" + isDuplicate);
                //If not duplicate then isDuplicate == false:
                if (!isDuplicate) {
                    //write the message attributes to a Hash to be indexed by Search
                    //use the eventID from the stream as the UID in the hash keyname:
                    String uid = consumedMessage.getId().toString();
                    System.out.println("EntryID = " + uid);
                    String singer = consumedMessage.getMessage().get("singer");
                    String album = consumedMessage.getMessage().get("album");
                    String song = consumedMessage.getMessage().get("song");
                    String lyrics = consumedMessage.getMessage().get("lyrics");
                    String releaseDate = consumedMessage.getMessage().get("releaseDate");
                    String timeOfArrival = uid.split("-")[0];
                    String isTombstoned = "false";
                    String isQueued = "false";
                    String isThrottled = "false";
                    if (x % 20 == 0) {
                        isThrottled = "true";
                    }
                    Map map = Map.of("singer", singer, "album", album, "song", song, "releaseDate",
                            releaseDate, "lyrics", lyrics, "TimeOfArrival", timeOfArrival,
                            "isTombstoned", isTombstoned, "isQueued", isQueued, "isThrottled", isThrottled);
                    connection.hset("song:" + uid, map);
                    /**
                     * Next section focuses on moving searched/sorted/filtered entries into the
                     * FairTopic...
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
                    boolean entityExists = true;
                    String hashKeyNameToProcessNext = "";
                    int retryCount = 10; //retry 10 times to allow time for new entries to arrive
                    while ((entityExists) && (retryCount > 0)) {
                        hashKeyNameToProcessNext = searcher.performSearchGetResultHashKey(connection, SEARCH_IDX, 1);
                        /*
                        //Dedup the song_album_singer using CuckooFilter
                        if (!connection.exists(CF_PROCESSING_TARGET_KEY_NAME)) {
                            connection.cfReserve(CF_PROCESSING_TARGET_KEY_NAME, cfReserveSize);
                        }
                        isNewTargetKey = connection.cfAddNx(CF_PROCESSING_TARGET_KEY_NAME, hashKeyNameToProcessNext);
                        */
                        //dedup the song_album_singer using SortedSet with 5 min window (300 seconds:
                        entityExists=SlidingWindowHelper.itemExistsWithinTimeWindow("Z:"+CF_PROCESSING_TARGET_KEY_NAME,hashKeyNameToProcessNext,connection,300);
                        System.out.println("Hmmm...  entityExists ==" + entityExists+ " "+hashKeyNameToProcessNext);
                        retryCount--;
                    }
                    //Process the Song Event (not necessarily the latest one just received)
                    //second topic is populated with fairly distributed events across all albums and singers
                    String fairSingerName = connection.hget(hashKeyNameToProcessNext, "singer");
                    if(null!=fairSingerName) {
                        fairProducer.produce(Map.of("keyName", hashKeyNameToProcessNext, "singer", fairSingerName));

                        //TimeSeriesEventLogger uses JedisPooledHelper to init JedisPooled connection when created:
                        //To make logger more robust you can assign startUpArgs to the instance (not done in this example)
                        TimeSeriesEventLogger logger = new TimeSeriesEventLogger().setSharedLabel("fair_events").
                            setCustomLabel(fairSingerName).
                            setTSKeyNameForMyLog("TS:" + fairSingerName).
                            initTS();
                        logger.addEventToMyTSKey(1.0d);
                        connection.hset(hashKeyNameToProcessNext, "isQueued", "true");
                        shouldAck = true;
                    }
                } else { // its a duplicate song for that artist and album!
                    shouldAck = true;
                }
                if (shouldAck) {
                    // To acknowledge a message, create an AckMessage:
                    AckMessage ack = new AckMessage(consumedMessage);
                    boolean success = consumer.acknowledge(ack);
                    System.out.println("Inbound song to be processed Message Acknowledged... " + ack);
                }
            }// end of if(!null==consumedMessage)
        }//end of convertTargetCount loop
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
        //ArrayList<String> argList = new ArrayList<>(Arrays.asList(args));
        /**
        if (argList.contains("--eventcount")) {
            int argIndex = argList.indexOf("--eventcount");
            howManySongEvents = Integer.parseInt(argList.get(argIndex + 1));
        }*/
        TopicProducer producer = new TopicProducer(connection,INBOUND_TOPIC_NAME);
        NewSongEventWriter nsew = new NewSongEventWriter();
        for (int x = 0; x < howManySongEvents; x++) {
            nsew.publishNewSongToTopic(producer);
        }
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