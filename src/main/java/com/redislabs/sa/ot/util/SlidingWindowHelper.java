package com.redislabs.sa.ot.util;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import java.util.List;

public class SlidingWindowHelper{

    public static boolean itemExistsWithinTimeWindow(String zKeyName,String entity,JedisPooled connection,long timeWindowSeconds){
        Pipeline pipeline = connection.pipelined();
        Response<List<String>> redisResponse = pipeline.time();
        pipeline.sync();
        pipeline.close();
        String redisTime = redisResponse.get().get(0);
        long timeSecondsNow = Long.parseLong(redisTime);
        //trim entries older than timeSecondsNow-timeWindow
        long countRemoved = connection.zremrangeByScore(zKeyName, 0.0d, (timeSecondsNow-timeWindowSeconds));
        //status == 1 if the element was new:
        //score is updated to reflect latest score(time) in any case:
        int status = (int)connection.zadd(zKeyName,timeSecondsNow,entity);
        boolean existedWithinTimeWindow = false;
        if(status==0){
            existedWithinTimeWindow=true;
        }
        return existedWithinTimeWindow;
    }
}