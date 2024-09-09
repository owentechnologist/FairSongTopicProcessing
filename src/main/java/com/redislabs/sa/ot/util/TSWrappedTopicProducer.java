package com.redislabs.sa.ot.util;
import com.redis.streams.command.serial.*;
import com.redis.streams.exception.InvalidMessageException;
import com.redis.streams.exception.InvalidTopicException;
import com.redis.streams.exception.ProducerTimeoutException;
import com.redis.streams.exception.TopicNotFoundException;

/**
 * This class wraps the TopicProduer.produce behavior with a call to
 * Redis TimeSeries adding a single record to the relevant TS key
 * To Query for the results you need to know the shared label used.
 * You can find that by executing TS.INFO against one of the TS keynames in Redis
 * Then issue a query like this:
 * TS.MRANGE - + AGGREGATION count 1000 FILTER sharedlabel=inbound_events GROUPBY uid reduce avg
 */

public class TSWrappedTopicProducer{
    TopicProducer topicProducer = null;
    String interestingAttributeNameForEntries = null;
    String sharedTSLabel = null;

    public TSWrappedTopicProducer setTopicProducer(TopicProducer topicProducer){
      this.topicProducer=topicProducer;
      return this;
    }

    /**
     * The values pointed to by this keyName which will be found in the entries
     * are used in the creation and population of the TimeSeries Keys
     * that enable tracking of the data entered through this wrapper
     * @param interestingKeyName
     * @return
     */
    public TSWrappedTopicProducer setInterestingAttributeNameForEntries(String interestingAttributeName){
        this.interestingAttributeNameForEntries=interestingAttributeName;
        return this;
    }

    /**
     * In order to see across multiple TS keys when querying
     * They must share a common label
     * @param sharedLabel
     */
    public TSWrappedTopicProducer setSharedTSLabel(String sharedLabel){
        this.sharedTSLabel=sharedLabel;
        return this;
    }

    /**
     * This method takes the opportunity to log the Topic entry using TimeSeries
     * the data that makes up the new Topic Entry is in the map/inputs
     * @param inputs
     */
    public void produceWithTSLog(java.util.Map inputs)throws TopicNotFoundException,InvalidMessageException,ProducerTimeoutException{
        String valueFromEntry = ""+inputs.get(this.interestingAttributeNameForEntries);
        //TimeSeriesEventLogger uses JedisPooledHelper to init JedisPooled connection when created:
        //To make logger more robust you can assign startUpArgs to the instance (not done in this example)
        TimeSeriesEventLogger logger = new TimeSeriesEventLogger().setSharedLabel(sharedTSLabel).
                setCustomLabel(valueFromEntry).
                setTSKeyNameForMyLog("TS:" + sharedTSLabel + valueFromEntry).
                initTS();
        topicProducer.produce(inputs);
        logger.addEventToMyTSKey(1.0d);
    }

}