package com.redislabs.sa.ot.fstp;

import redis.clients.jedis.search.*;
import redis.clients.jedis.search.FieldName;
import redis.clients.jedis.search.aggr.*;
import java.util.ArrayList;
import redis.clients.jedis.*;


public class SearchHelper{
    ArrayList<String> groupByFields = new ArrayList<>();
    ArrayList<Reducer> reducerCollection = new ArrayList<>();
    String queryGuts = "";
    /**
     * Search query looks like this:
     * FT.AGGREGATE idx_songs "@isQueued:{false} @isThrottled:{false} @isTombstoned:{false}"
     * LOAD 2 @__key @TimeOfArrival groupby 3 @__key @singer @album
     * reduce MAX 1 TimeOfArrival AS 'TOA' SORTBY 2 @TOA ASC LIMIT 0 10
     */
    public SearchHelper(){
        this.groupByFields.add("@__key");
        this.groupByFields.add("@singer");
        this.groupByFields.add("@album");
        this.reducerCollection.add(Reducers.max("TimeOfArrival").as("TOA"));
        this.queryGuts="@isQueued:{false} @isThrottled:{false} @isTombstoned:{false}";
    }
    /**
     *          ArrayList<String> groupByFields = new ArrayList<>();
     *          groupByFields.add("@cost");
     *          groupByFields.add("@location");
     *          groupByFields.add("@event_name");
     *          ArrayList<Reducer> reducerCollection = new ArrayList<>();
     *          reducerCollection.add(Reducers.count().as("event_match_count"));
     *          String queryGuts = "@event_name:Petting @cost:[1.00 +inf] " +
     *                  "@location:Gorilla @location:East -@days:{Tue Wed Thu}";
     *          AggregationBuilder builder = new AggregationBuilder(queryGuts)
     *                  .groupBy(groupByFields,reducerCollection).filter("@cost <= 9").dialect(Main.dialectVersion);
     */
    public String performSearchGetResultHashKey(JedisPooled connection,String idxName,int limitMax){
        long startTime=System.currentTimeMillis();
        //sortBY(). SHOULD WE SORTBY TOA?
        AggregationBuilder builder = new AggregationBuilder(this.queryGuts).
                load("@__key", "@TimeOfArrival").
                groupBy(this.groupByFields,this.reducerCollection).
                sortBy(SortedField.asc("@TOA")).
                limit(0,limitMax).dialect(2);//dialect 3 not needed for this query
        AggregationResult ar = connection.ftAggregate(idxName,builder);
        boolean isArraylist = false;
        if(ar.getTotalResults()<1){
            java.util.Map<String,Object> m = connection.ftInfo(idxName);
            Object s=m.get(0);
            try {
                ArrayList<String> al = (ArrayList<String>) s;
                isArraylist=true;
            }catch(Throwable t){t.printStackTrace();}
            if(isArraylist){
                System.out.println(s+"  No results this time... is there data in Redis?");
            }else{
                throw new RuntimeException("\n\nSEARCH INDEX MISSING!\nYou must create the index using: \n"+
                        "\"FT.CREATE idx_songs prefix 1 song SCHEMA singer TAG SORTABLE album TAG SORTABLE isTombstoned TAG isQueued TAG isThrottled TAG TimeOfArrival NUMERIC SORTABLE\""+
                        "\n");
            }
        }
        String hashKeyName = "no result";
        if(!isArraylist) {
            hashKeyName = ar.getRow(0).getString("__key");
        }
        System.out.println("\nSearch took "+(System.currentTimeMillis()-startTime)+" millis... Result: keyname == "+hashKeyName);

        return hashKeyName;
    }
}