#!/usr/bin/env python
"""
	Process Traffic data. When an incident is reported or clear we simply
	push iti the Redis key "raw_input". Likewse for user location update.
	This data is then analyzed by an offline process.	
"""
import pyspark_cassandra
import pyspark_cassandra.streaming

from pyspark_cassandra import CassandraSparkContext

from pyspark.sql import SQLContext
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from uuid import uuid1

import json
import redis
import sys
import datetime
import random
from random import randrange

TYPE_CLEAR, TYPE_INCIDENT, TYPE_USERS = range(3)
TS_FMT = '%Y-%m-%d %H:%M:%S'


def init_route_inc_ids(redis_conn):
	for i in range(100):
		route_inc_id = "route_incident_num%d" % i
		redis_conn.set(route_inc_id, 0)

def processAccident(iter):
	redis_conn = redis.Redis(host = redis_host, port = 6379, password = 'noredishackers')
	for record in iter:
		process_accident(redis_conn, record)

def processUserPos(iter):
	redis_conn = redis.Redis(host = redis_host, port = 6379, password = 'noredishackers')
	for record in iter:
		user_pos_update(redis_conn, record)


def process_accident(redis_conn, user_req):
	accident_occur_clear = user_req[1].split()[3]
	redis_conn.rpush("raw_input", ' '.join([accident_occur_clear, user_req[0], user_req[1]]))

def user_pos_update(redis_conn, user_req):
	redis_conn.rpush("raw_input", ' '.join([str(TYPE_USERS), user_req[0], user_req[1]]))

sample_secs = int(sys.argv[1])
redis_host = sys.argv[2]
spark_host = sys.argv[3]
conf = SparkConf() \
    .setAppName("PySpark Redis Test") \
    .setMaster("spark://"+spark_host+":7077")

redis_conn = redis.Redis(host = redis_host, port = 6379, password = 'noredishackers')
init_route_inc_ids(redis_conn)
sc = SparkContext(conf=conf)
stream = StreamingContext(sc, sample_secs) # window

metalist = ','.join([ip + ":9092" for ip in sys.argv[3:]])
directKafkaStream_accident = KafkaUtils.createDirectStream(stream, ["user_accident"], {"metadata.broker.list": metalist})
directKafkaStream_pos = KafkaUtils.createDirectStream(stream, ["user_pos"], {"metadata.broker.list": metalist})

user_accident = directKafkaStream_accident.map(lambda (k,v): json.loads(v)).map(lambda(v): (v['incident_route_num'], ' '.join([v['incident_loc'], v['ts'], v['occur_clear'], v['accident_duration']])))
user_accident.foreachRDD(lambda rdd: rdd.foreachPartition(processAccident))

user_pos = directKafkaStream_pos.map(lambda (k,v): json.loads(v)).map((lambda(v):(v['user_route_num'], ' '.join([v['user_loc'], v['user_id'], v['ts']]))))
#user_pos.pprint(num=1)
user_keyed = user_pos.combineByKey(lambda v: v, lambda a, b: a + ' ' + b, lambda a, b : a + ' ' + b)
user_keyed.foreachRDD(lambda rdd: rdd.foreachPartition(processUserPos))
#user_keyed.pprint()

stream.start()
stream.awaitTermination()
