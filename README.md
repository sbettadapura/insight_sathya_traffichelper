# TrafficHelper

### Overview

Commuters (users of traffichelper) send continual updates about their positions on the roads and also the occurence and clearence of incidents that can hinder traffic. Users receive alerts whenever an incident occurs ahead of them within a pre-determined distance from where they are.

### Traffic Synthesis

Traffic is simulated by a network of 100 roads of random sizes
between 13 and 23. Each road begins at location "x" and consists of 
consecutive  segments ending at "x" + n where n >= 13 and < 23. 

An incident is reported as (road_number, location, timestamp). A user location
update is reported as (road_number, location, userid, timestamp).
Approximately 2% incident occurences and 2% clearances are randomly generated compared to user location updates. Generation of incident occurences is completely random.
When we eventually process them, we discard events that don't make sense, i.e.
if we see an incident being cleared when none has been reported, we discard
it.

The traffic synthesizer puts out a JSON object with the above fields to two 
Kafka streams, "user_pos" and "user_accident" both with 18 partitions.

### The problem

The problem is to do an efficient join of these two streams and extract a set
of users who are on a given road where an incident has occured. From this
set of users we determine a subset of users that we consider "affected" who 
we send notifications to. A user is considered "affected" if he's between 0 
and 2 "locations" behind the location of the incident; a road is represented 
as a collection of consecutive segments with increasing location numbers; 
location nunbers are unique in the network of roads being simulated.

### Design Choices

Two designs were explored. Spark Stream technology has a mechanism to 
join two Dstreams. Thus this seemed like an obvious choice to explore.

### Choice 1: Spark Stream Dstream Join

As mentioned above an incident comes in as a JSON object with 
(road, incdent, timestamp) vector. A user location update comes in as 
(road, location, user_id, timestamp). From the incident stream we create
a Dstream with "road" as the key and (incident, tmestamp) as the value.
Fron the user-location-update we put out a Dstream with road as the key
and (location, user_od, timestamp) as the value. When the Dstream is joined
we get one (k, v) pair where k = road and v = (incident_loc, timestamp, 
user_loc1, user_id1, timestamp1, user_loc2, user_id2, timestamp2, ...)
This structure is written out to a Redis database for offline processing.

While this approach works, it involves a fair amount of shuffle. To minimize this, a better approach is to write the (k, v) pairs from Spark Partitions directly without merging them in the reduce phase. The following choice describes it.

### Choice 2: Manual Join

In this alternative, we want to do a join of incident and user-location-updates locally on each Spark Partition and write it out to Redis. We keep processing he incident stream by simply recording it in the Redis database with a (road, (locatiobn, ts)). On the user-location-update stream, we do a 'combineByKey' locally to produce a (road, (location1, user_id1, ts1, location2, user_id2, ts2,...) tuple. Once we have these tuples, we can simply compare each individual user location to the location of the incident and consider the user 'affected' if it meets our criterion that the user location is between 0 and 2 locations behind the incident location.

### Design Chosen

The Manual Join approach reduces both processing time and Shuffle band-width as can be seen in the section on Performance. If a Map-Only job were possible, it would do even better as the Shuffle should disappear completely. This is the design of choice.

### Limitations

There were a number of road-blocks in this approach:
* A pySpark bug which appears to be related to the time taken to process user-location-updates. A well-behaved Kafka consumer should simply put back-pressure on the producer, but instead, pySpark runs into an empty RDD on the incident stream which causes an exception. To work-around this bug, the processing of the data received in the streams has been minimized by just writing them out to Redis and the data is batch-processed offline.

* Spark does not seem to allow a Map-only or more precisely a Map-Combine-Only job. So after a local 'combineByKey' we're forced to do a global 'combineByKey' which incurs unnecessary shuffle.

* There's no Spark-Redis connector available in Python, with the result that we have to write to Redis manually, by establishing connections to Redis on each worker node on every batch of RDDs. Once the data is written to Redis, we can not do a Spark batch process for lack of Spark-Redis connector. So we settle for an offline process in Python running on a single node to analyze the data. This limitation applies to the Dstream Join approach as well.

### Data Pipeline

The data pipeline for the above task is as below.
<p align="center">
<img src="/images/data_pipeline.png" width="650"/>
</p>

### How to run the program


traffic_gen.py - Generates input.
traffic_process.py - Processes the input and populates data in a Redis database.
                     (It uses a bash script called mypark.sh to do a 
			"spark-submit" with the needed packages). 

offline_process.py - Analzyes the data offline. 

All the above ( generator, processor and offline processors) are run at the same time.

run_py in the directory flask_dir,  provides UI.
The following environment variables need to be defined for access to AWS Redshift.

export REDSHIFT_HOST=<host>
export REDSHIFT_USER=<user>
export REDSHIFT_PASSWD=<password>
export REDSHIFT_DATABASE=<database>

### Results of some Queries

A number of queries can be run once user data has been processed. Here are a few examples:

<p align="center">
<img src="/images/users.png" width="650"/>
</p>

<p align="center">
<img src="/images/incidents.png" width="650"/>
</p>

<p align="center">
<img src="/images/conditions_on_66.png" width="650"/>
</p>

### Performance

<p align="center">
<img src="/images/performance.png" width="650"/>
</p>

