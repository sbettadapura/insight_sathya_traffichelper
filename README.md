# TrafficHelper

### Overview

Users send continual updates about their positions on the roads and also the occurence and clerence of incidents that can hinder traffic. Users receive alerts whenever an incident occurs within a pre-determined distance from where they are.

### Traffic Synthesis

We simulate traffic by simulating a network of 100 roads of random sizes
between 13 and 23. Each road begins at location "x" and consists of 
consecutive  segments ending at "x" + n where n >= 13 and < 23. 

An incident is reported as (road_number, location, timestamp). A user location
update is reported as (road_number, location, userid, timestamp). We generatd
approximately 2% incident occurences and 2% clearnces compared to user
loation updates. Generation of incidents occurence is completely random.
When we eventualy process them we discard evens that don't make sense, i,e
if we see an incident being cleared when none has been reported, we discard
it.

The traffic synthesizer puts out a JSON object with the above fileds to two 
Kafka streams, "user_pos" and "user_accident" both wth 18 partitions.

### The problem

The problem is to do an efficient join of these two streams and extract a set
of users who're on a given road where an incident has occured. From this
set of users we determine a subset of users that we consider "affected" who 
we send notification to. A user is considered "affected" if he's between 0 
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
a Dstream with "road" as the key and (incdent, tmestamp) as the value.
Fron the user-location-update we put out a Dstream with road as the key
and (location, user_od, timestamp) as the value. When the Dstream is joined
we get one (k, v) pair where k = road and v = (incident_loc, timestamp, 
user_loc1, user_id1, timestamp1, user_loc2, user_id2, timestamp2, ...)
This structure is written out to a Redis database for offline processing.
While this approach works, it involves a fair amount of shuffle. Wed prefer to write the (k, v) pairs from Spark Partitions directly without merging them in the reduce phase. So we consider another approach.

### Choice 2: Manual Join

In this alternative, we want to do a join of incident and user-location-update locally on each Spark Partition and write it out to Redis. We keep processing he incident stream by simply recording it in teh Redis database with a (road, (locatiobn, ts)). On the user-location-update stream, we do a 'combineByKey' locally to produce a (road, (location1, user_id1, ts1, location2, user_id2, ts2,...) tuple. Once we have these tuples, we can simply compare each individual user location to the location of the incident and consider the user 'affected' if it meets our criterion that the use location is between 0 and 2 locations behind the incident location.

### Design Chosen

The Manual Join approach reduces both processing time and Shuffle band-width as can be seen in the section on Performance. If we figure out a way to do a Map-Only job, we'll do even better as the Shuffle should disappear completely. This is the design of choice.

### Limitations

We met with a number of road-blocks in this approach. 
* We run into a pySpark bug which appears to be related to the time we process user-location-updates. A well-behaved Kafka consmer should simply put back pressure on the producer, but instead, pySpark runs into an empty RDD on the incident stream which causes an exception. To work-around this bug, we minimize the processing of the data received in the streams by just writing them out to Redis and batch-process the data.

* What we have is essentially a Map-Only or more precisely a Map-Combine-Only job. It looks like Spark does not let us do that. So after a local 'combineByKey' we're forced to do a global 'combineByKey' which incurs unnecessary shuffle.

* There's no Spark-Redis connector available in Python, with the result that we have to write to Redis manually, by establishing connections to Redis on each worker node on every batch of RDDs. Once the data is written to Redis, we can not do a Spark batch process for lack of Spark-Redis connector. So we settle for an offline process in Python running on a single node to analyze the data. This limitation applies to the Dstream Join approach as well.

### Data Pipeline

The data pipeline for the above task is as below.
<p align="center">
<img src="/images/data_pipeline.png" width="650"/>
</p>

### How to run the program

We synthesize traffic updates from a network consisting of 100 roads of 
arbitrary lengths.

Input is generated by running traffic_gen.py. 
The input that's geerated is processed by running traffic_process.py via
a bash script called mypark.sh. myspark.sh does "spark-submit" with the needed
packages. traffic_process.py populates data in a Redis database.

Ths data is analyzed by a Python program offine_process.py. You run the 
generator, processor and offline processors all at the same time.

In the directory flask_dir, run "run.py" for UI.
In your profile define the following for access to AWS Redshift.

export REDSHIFT_HOST=<host>
export REDSHIFT_USER=<user>
export REDSHIFT_PASSWD=<password>
export REDSHIFT_DATABASE=<database>

### Performance
<p align="center">
<img src="/images/performance.png" width="650"/>
</p>

### Results of some Queris

A number of queries can be run nc we've processed user data. Here're a few below.
<p align="center">
<img src="/images/users.png" width="650"/>
</p>

<p align="center">
<img src="/images/incidents.png" width="650"/>
</p>

<p align="center">
<img src="/images/conditions_on_66.png" width="650"/>
</p>
