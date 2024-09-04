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

public class TopicEntriesTopKAuditorThread extends Thread{

    String topicName;
    String consumerGroupName;
    String consumerInstanceName;
    String attributeNameToTrack="album";
    JedisPooled connection;
    int numberOfMessagesToProcess=1;
    int topKSize=100;

    public TopicEntriesTopKAuditorThread setTopKSize(int topKSize){
        this.topKSize=topKSize;
        return this;
    }

    public TopicEntriesTopKAuditorThread setJedisPooledConnection(JedisPooled connection){
        this.connection=connection;
        return this;
    }

    public TopicEntriesTopKAuditorThread setTopicName(String topicName){
        this.topicName=topicName;
        return this;
    }

    public TopicEntriesTopKAuditorThread setAttributeNameToTrack(String attributeNameToTrack){
        this.attributeNameToTrack=attributeNameToTrack;
        return this;
    }

    public TopicEntriesTopKAuditorThread setConsumerGroupName(String consumerGroupName){
        this.consumerGroupName=consumerGroupName;
        return this;
    }

    public TopicEntriesTopKAuditorThread setConsumerInstanceName(String consumerInstanceName){
        this.consumerInstanceName=consumerInstanceName;
        return this;
    }

    public TopicEntriesTopKAuditorThread setNumberOfMessagesToProcess(int numberOfMessagesToProcess){
        this.numberOfMessagesToProcess=numberOfMessagesToProcess;
        return this;
    }

    /**
     * new Thread(TopicConsumer).start() kicks this off...
     */
    public void run(){
        if(null==this.connection){
            throw new RuntimeException("Check that all attributes are set... \n" +
                    "Must Set consumerInstanceName, consumerGroupName, topicName, and connection");
        }
        try{
            ConsumerGroup consumer = new ConsumerGroup(connection, this.topicName, this.consumerGroupName);
            TopKHelper topKHelper = new TopKHelper().setTopKSize(this.topKSize).setJedis(this.connection).setTopKKeyName("TK:"+this.attributeNameToTrack+":"+this.topicName);
            for(int x= 0;x<numberOfMessagesToProcess;x++) {
                TopicEntry consumedMessage = consumer.consume(consumerInstanceName);
                if (!(null == consumedMessage)) {
                    consumedMessage.getMessage().forEach(
                            (key, value) -> System.out.println(key + ":" + value)
                    );
                    String topKValue = consumedMessage.getMessage().get(this.attributeNameToTrack);
                    topKHelper.addEntryToMyTopKKey(topKValue);

                    AckMessage ack = new AckMessage(consumedMessage);
                    boolean success = false;
                    while (!success) {
                        success = consumer.acknowledge(ack);
                        Thread.sleep(100);//wait between tries
                    }
                }
            }
        }catch(Throwable t){
                t.printStackTrace();
        }
    }
}