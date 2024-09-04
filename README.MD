This project demonstrates using Redis Streams and Search and SortedSets to solve the problem of fairly processing events across a range of groupings.

This project also uses the TimeSeries and TopK datatypes to track the arrival time, processing rate, and count for each singer grouping.

* here are some queries for the TimeSeries datatype:
NB: These keys only get generated when you pass --converttofair true
```
TS.MRANGE - + AGGREGATION count 60000 FILTER sharedlabel=fair_events GROUPBY uid reduce avg
```
```
TS.MRANGE - + AGGREGATION count 6000 FILTER sharedlabel=fair_events GROUPBY uid reduce avg
```
```
TS.MRANGE - + AGGREGATION count 1000 FILTER sharedlabel=fair_events GROUPBY uid reduce avg
```

* and here are queries against the TopK datatype:
NB: these keys only get populated when you pass --audittopics true

```
TOPK.LIST TK:singer:InboundSongsTopic withcount
```

```
TOPK.LIST TK:singer:FairProcessingTopic withcount
```

The goal is to minimize the chance that one grouping grabs the lion's share of processing time at the expense of other groupings.

The premise of this project is the following:
Imagine you have incoming songs that need to be 'processed' in some way.
The songs are grouped by singer and album.
The goal is to ensure that one singer or album does not get an unfair amount of processing as compared to the others.
* There will always be at most 15 singers,
* 100 albums per singer,
* and at most 1,000 songs per album.

To run the program you will need a Redis instance with the following modules enabled:
Bloom, Search, TimeSeries

You can spin up a free redis database with these capabilities in the cloud here:  https://redis.io/try-free/

This example includes a Java library that enables the Topic abstraction.

* Execute the mvn command with necessary args to connect to your Redis instance

* Example with all required args publishing only new Song entries:
```
mvn compile exec:java -Dexec.cleanupDaemonThreads=false -Dexec.args="--host redis-FIXME.com --port 0000 --password FIXME --eventcount 1500 --publishnew true --converttofair false --convertcount 0 --audittopics false"
```
* Example with all required args converting Song entries to FairEntries:
```
mvn compile exec:java -Dexec.cleanupDaemonThreads=false -Dexec.args="--host redis-FIXME.com --port 0000 --password FIXME --eventcount 1500 --publishnew false --converttofair true --convertcount 10 --audittopics false"
```
* Example with all required args producing topK results from INBOUND and FAIR Topics:
```
mvn compile exec:java -Dexec.cleanupDaemonThreads=false -Dexec.args="--host redis-FIXME.com --port 0000 --password FIXME --eventcount 1500 --publishnew false --converttofair false --convertcount 0 --audittopics true"
```

* It is expected that the Search module is enabled and the following index has been created...
* Execute the following from redis-cli / redisInsight to create the necessary index:
```
FT.CREATE idx_songs prefix 1 song SCHEMA singer TAG SORTABLE album TAG SORTABLE isTombstoned TAG isQueued TAG isThrottled TAG TimeOfArrival NUMERIC SORTABLE
```
