package com.redislabs.sa.ot.fstp;

import net.datafaker.providers.base.*;
import net.datafaker.*;

import com.redis.streams.*;
import com.redis.streams.command.serial.*;
import com.redis.streams.exception.InvalidMessageException;
import com.redis.streams.exception.InvalidTopicException;
import com.redis.streams.exception.ProducerTimeoutException;
import com.redis.streams.exception.TopicNotFoundException;
import java.util.Map;

/**
 *  * There will always be at most 15 singers,
 *  * 100 albums per singer,
 *  * and at most 1,000 songs per album.
 */
public class NewSongEventWriter{
    Faker faker = new Faker();
    String[] loveHate = new String[]{"Painful Love Hurts ","Sweet Love Heals "};
    String[] singerNames = new String[]{
            "Billie Eilish","Elvis","Cher","Stevie Wonder","Sting",
            "Peter Gabriel","Elton John","Freddie Mercury","John Legend","Taylor Swift",
            "Rhianna","Madonna","Lady Gaga","Ariana Grande","Alicia Keys"
    };

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
        //need to create a larger number of songs from 3 artists to see if
        // fairness can prevail even as more songs come in from certain 'channels'
        String singerName = singerNames[(int)(System.nanoTime() % 3)];
        if(System.currentTimeMillis()%2==0) {
            singerName = singerNames[(int) (System.nanoTime() % 15)];
        }
        String titan = this.faker.ancient().titan();//roughly 34 of these
        String albumName = singerName+" presents "+buzzWord+" "+loveType;//~100 of these per singer
        String song = titan+" "+foodName;
        String lyrics = loveType+" "+this.faker.food().spice()+" "+loveType+" "+this.faker.text().text(30,2000);
        String releaseDate = "139"+(1924967499+((10000000*(System.nanoTime()%1000))));
        producer.produce(Map.of("album",albumName,"singer",singerName,"song",song,"lyrics",lyrics,"releaseDate",releaseDate));
    }
}