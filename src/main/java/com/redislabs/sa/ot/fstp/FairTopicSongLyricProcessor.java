package com.redislabs.sa.ot.fstp;

import com.redislabs.sa.ot.util.*;
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

public class FairTopicSongLyricProcessor extends Thread{

    String topicName;
    String consumerGroupName;
    String consumerInstanceName;
    String attributeNameToTrack;
    JedisPooled connection;
    int numberOfMessagesToProcess=1;


    public FairTopicSongLyricProcessor setJedisPooledConnection(JedisPooled connection){
        this.connection=connection;
        return this;
    }

    public FairTopicSongLyricProcessor setTopicName(String topicName){
        this.topicName=topicName;
        return this;
    }

    public FairTopicSongLyricProcessor setAttributeNameToTrack(String attributeNameToTrack){
        this.attributeNameToTrack=attributeNameToTrack;
        return this;
    }

    public FairTopicSongLyricProcessor setConsumerGroupName(String consumerGroupName){
        this.consumerGroupName=consumerGroupName;
        return this;
    }

    public FairTopicSongLyricProcessor setConsumerInstanceName(String consumerInstanceName){
        this.consumerInstanceName=consumerInstanceName;
        return this;
    }

    public FairTopicSongLyricProcessor setNumberOfMessagesToProcess(int numberOfMessagesToProcess){
        this.numberOfMessagesToProcess=numberOfMessagesToProcess;
        return this;
    }

    /**
     * new Thread(fairTopicSongLyricProcessor).start() kicks this off...
     */
    public void run(){
        if((null==connection)||
                (null==consumerGroupName)||
                (null==consumerInstanceName)||
                (null==topicName)){
            throw new RuntimeException("\n\tFairTopicSongLyricProcessor---> MISSING PROPERTIES - you must set all properties before starting this Thread.");
        }
        try{
            ConsumerGroup consumerGroup = new ConsumerGroup(connection, this.topicName, this.consumerGroupName);
            long lag = 1;
            while((lag>0)||(numberOfMessagesToProcess>0)){
                //try to consume 10 events before checking lag
                for(int l=0;l<10;l++){
                    TopicEntry consumedMessage = consumerGroup.consume(consumerInstanceName);
                    if (!(null == consumedMessage)) {
                        System.out.println("singer" + ":" + consumedMessage.getMessage().get("singer"));
                        processLyrics(consumedMessage);
                        AckMessage ack = new AckMessage(consumedMessage);
                        boolean success = false;
                        while (!success) {
                            success = consumerGroup.acknowledge(ack);
                            Thread.sleep(100);//wait between tries
                            numberOfMessagesToProcess--;
                        }
                    }
                }
                lag=((ConsumerGroupBase)consumerGroup).getCurrentLagForThisInstanceTopicAndGroup();
            }
        }catch(Throwable t){
            t.printStackTrace();
        }
    }

    void processLyrics(TopicEntry consumedMessage){
        String hashKeyName =  consumedMessage.getMessage().get("keyName");
        String lyricKeyName = connection.hget(hashKeyName,"lyricsKey");
        int lyricsLength = Integer.parseInt(connection.hget(hashKeyName,"lyricsLength"));
        while(connection.zcard(lyricKeyName)<lyricsLength){
            try{
                Thread.sleep(50);
                System.out.println("FairTopicSongLyricProcessor.processLyrics() waiting for all lyrics to arrive... for song: "+
                        consumedMessage.getMessage().get("song"));
            }catch(Throwable t){}
        }
        String lyrics = "";
        java.util.List<String> lyricsList = connection.zrange(lyricKeyName,0,lyricsLength);
        for(int x=0;x<lyricsList.size();x++){
            lyrics+=lyricsList.get(x).split(":")[1];
            lyrics+=" ";
        }
        connection.hset(hashKeyName,"lyrics",lyrics);
    }
}