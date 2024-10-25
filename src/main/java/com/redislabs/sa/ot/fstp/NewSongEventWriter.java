package com.redislabs.sa.ot.fstp;

import net.datafaker.providers.base.*;
import net.datafaker.*;

import com.redislabs.sa.ot.util.TSWrappedTopicProducer;
import com.redis.streams.*;
import java.util.Random;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.Pipeline;

import com.redis.streams.command.serial.*;
import com.redis.streams.exception.InvalidMessageException;
import com.redis.streams.exception.InvalidTopicException;
import com.redis.streams.exception.ProducerTimeoutException;
import com.redis.streams.exception.TopicNotFoundException;
import java.util.Map;
import java.util.logging.*;

/**
 *  * There will always be at most 15 singers,
 *  * 100 albums per singer,
 *  * and at most 1,000 songs per album.
 */
public class NewSongEventWriter extends Thread{

    static Logger logger = Logger.getLogger("com.redislabs.sa.ot.fstp.NewSongEventWriter");
    TopicProducer topicProducer = null;
    Random nsewRandom = new Random();
    JedisPooled connection = null;
    Integer howManySongEvents = null;
    Integer sleepMillis = null;
    Faker faker = new Faker();
    String[] loveHate = new String[]{"Painful Love Hurts ","Sweet Love Heals "};
    String[] singerNames = new String[]{
            "Elvis","Cher","Stevie Wonder","Billie Eilish","Sting",
            "Peter Gabriel","Elton John","Freddie Mercury","John Legend","Taylor Swift",
            "Rhianna","Madonna","Lady Gaga","Ariana Grande","Alicia Keys"
    };

    public NewSongEventWriter setHowManySongEventsToWrite(int howManySongEventsToWrite){
        this.howManySongEvents=new Integer(howManySongEventsToWrite);
        return this;
    }
    public NewSongEventWriter setTopicProducer(TopicProducer topicProducer){
        this.topicProducer=topicProducer;
        return this;
    }
    public NewSongEventWriter setSleepMillisBetweenWrites(int sleepMillisBetweenWrites){
        this.sleepMillis=new Integer(sleepMillisBetweenWrites);
        return this;
    }
    public NewSongEventWriter setJedisPooled(JedisPooled connection){
        this.connection = connection;
        return this;
    }

    public void run(){
        if((null==topicProducer)||
                (null==howManySongEvents)||(null==sleepMillis)
                ||(null==connection)){
            throw new RuntimeException("\n\t---> MISSING PROPERTIES - you must set all properties before starting this Thread.");
        }
        for (int x = 0; x < howManySongEvents; x++) {
            try{
                publishNewSongToTopic(topicProducer);
                //slow down the writing a bit to help with visibility into the behavior
                //ie: which singers are getting the lion's share of the song entries?
                Thread.sleep(sleepMillis);
            }catch(Throwable t){}
        }
    }


    /**
     * Figure out what exceptions are good to throw
     * @param topicName
     * HSET song:1724450223466-1
     * singer 'Cher'
     * album 'Believe'
     * song 'Believe'
     * TimeOfArrival '1724450223466.1'
     * isTombstoned false
     * isThrottled false
     * isQueued false
     * releaseDate '1720140299900'
     * lyrics 'No matter '
     */
    public void publishNewSongToTopic(TopicProducer producer)throws TopicNotFoundException,InvalidMessageException,ProducerTimeoutException{

        String foodName = this.faker.food().sushi();//37 of these
        String buzzWord = this.faker.marketing().buzzwords(); //roughly 50 of these
        String loveType = loveHate[(int)(System.nanoTime()%2)];
        //need to create a larger number of songs from 2 artists to see if
        // fairness can prevail even as more songs come in from certain 'channels'
        String singerName = singerNames[(int)(System.nanoTime() % 2)];
        if(System.currentTimeMillis()%10==0) { // the other singers have songs published
            singerName = singerNames[(int) ((System.nanoTime() % 13)+2)];
        }
        String titan = this.faker.ancient().titan();//roughly 34 of these
        String albumName = singerName+" presents "+buzzWord+" "+loveType;//~100 of these per singer
        String song = titan+" "+foodName;
        String lyricsKey = "Z:"+singerName+":"+albumName+":"+song;
        int lyricsLength = writeLyrics(lyricsKey,loveType);
        String releaseDate = "139"+(1924967499+((10000000*(System.nanoTime()%1000))));
        TSWrappedTopicProducer wrappedProducer = new TSWrappedTopicProducer().setTopicProducer(producer).
                setInterestingAttributeNameForEntries("singer").
                setSharedTSLabel("inbound_events");
        wrappedProducer.produceWithTSLog(Map.of("album",albumName,"singer",singerName,"song",song,"lyricsKey",lyricsKey,"lyricsLength",lyricsLength+"","releaseDate",releaseDate));

        //producer.produce(Map.of("album",albumName,"singer",singerName,"song",song,"lyricsKey",lyricsKey,"lyricsLength",lyricsLength,"releaseDate",releaseDate));
    }

    int writeLyrics(String lyricsKey,String loveType){
        String lyrics = loveType+this.faker.food().spice()+" "+loveType.trim();
        int songLength = Math.abs((20)+nsewRandom.nextInt(5000));
        for(int l = 0;l<songLength;l++) {
            lyrics += (" " + this.faker.hacker().adjective());
            lyrics += (" " + this.faker.mood().feeling());
            lyrics += (" " + this.faker.animal().name());
            lyrics += (" " + this.faker.country().name());
            lyrics += (" " + loveType.trim());
        }
        lyrics="__BEGIN__ "+lyrics+" __END__";
        String[] songLyrics = lyrics.split(" ");
        int lyricsLength = songLyrics.length;
        Pipeline pipe = connection.pipelined();
        for(int x=0;x<songLyrics.length;x++){
            pipe.zadd(lyricsKey,x,x+":"+songLyrics[x]);
        }
        pipe.sync();
        pipe.close();
        return lyricsLength;
    }
}