package com.redislabs.sa.ot.fstp;

import java.util.logging.*;
import redis.clients.jedis.search.*;
import redis.clients.jedis.search.FieldName;
import redis.clients.jedis.search.aggr.*;
import java.util.ArrayList;
import redis.clients.jedis.*;


public class SearchHelper{
    ArrayList<String> groupByFields = new ArrayList<>();
    ArrayList<Reducer> reducerCollection = new ArrayList<>();
    String queryGuts = "";
    static Logger logger = Logger.getLogger("com.redislabs.sa.ot.fstp.SearchHelper");

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
     * Searches for indexed Hash by various attributes
     * The one that mostly matters is @isQueued - which indicates that Hash has already been added
     * to the FairProcessingTopic and should not be included in search results
     */
    public ArrayList<String> performSearchGetResultHashKey(JedisPooled connection,String idxName,int limitMax){
        //System.out.println("Debug search limit arg == "+limitMax);
        long startTime=System.currentTimeMillis();
        //sortBY(). SHOULD WE SORTBY TOA?
        AggregationBuilder builder = new AggregationBuilder(this.queryGuts).
                load("@__key", "@TimeOfArrival").
                groupBy(this.groupByFields,this.reducerCollection).
                sortBy(SortedField.asc("@TOA")).
                limit(0,limitMax).dialect(2);//dialect 3 not needed for this query
        AggregationResult ar = connection.ftAggregate(idxName,builder);
        boolean isNoResult = false;
        if(ar.getTotalResults()<1){
            java.util.Map<String,Object> m = connection.ftInfo(idxName);
            Object s=m.get(0);
            try {
                ArrayList<String> al = (ArrayList<String>) s;
                isNoResult=true;
            }catch(Throwable t){t.printStackTrace();}
            if(isNoResult){
                logger.fine(s+"  No results this time... is there data in Redis?");
            }else{
                throw new RuntimeException("\n\nSEARCH INDEX MISSING!\nYou must create the index using: \n"+
                        "\"FT.CREATE idx_songs prefix 1 song SCHEMA singer TAG SORTABLE album TAG SORTABLE isTombstoned TAG isQueued TAG isThrottled TAG TimeOfArrival NUMERIC SORTABLE\""+
                        "\n");
            }
        }
        ArrayList<String> hashKeyNamesList = new ArrayList<String>();
        if(!isNoResult) { //we have results!
            for(int r=0;r<limitMax;r++){
                try{
                    hashKeyNamesList.add(ar.getRow(r).getString("__key"));
                }catch(Throwable t){}
            }
        }
        if(hashKeyNamesList.size()>0){
            logger.fine("\nSearch took "+(System.currentTimeMillis()-startTime)+" millis... Result: sample keyname == "+hashKeyNamesList.get(0));
        }
        return hashKeyNamesList;
    }
}