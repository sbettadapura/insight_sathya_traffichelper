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

def inc_key_if_exists(key):
	if redis_conn.keys(key):
		redis_conn.set(key, str(int(redis_conn.get(key)) + 1))
	else:
		redis_conn.set(key, str(0))

def dec_key_if_exists(key):
	if redis_conn.keys(key):
		redis_conn.set(key, str(int(redis_conn.get(key)) - 1))
	else:
		redis_conn.set(key, str(0))

def init_route_inc_ids(redis_conn):
	for i in range(100):
		route_inc_id = "route_incident_num%d" % i
		redis_conn.set(route_inc_id, 0)

def processAccident(iter):
	redis_conn = redis.Redis(host = redis_host, port = 6379, password = 'noredishackers')
	for record in iter:
		process_accident(redis_conn, record)

def dummyprocessUserPos(iter):
	pass

def processUserPos(iter):
	redis_conn = redis.Redis(host = redis_host, port = 6379, password = 'noredishackers')
	for record in iter:
		create_db_recs(redis_conn, record)

def create_db_recs(redis_conn, user_req):
	db_user_register(redis_conn, user_req)

def notify_users(users):
	pass

def notify_single_user(user):
	pass

def process_accident(redis_conn, user_req):
	#redis_conn.incr("incidents_recorded")
	#accident_route_num = user_req[0]
	#accident_loc = user_req[1][0]
	#accident_ts = user_req[1][1]
	accident_occur_clear = user_req[1].split()[3]
	#accident_duration = int(user_req[1][3])
	#accident_route_id = "incident_route%d" % int(accident_route_num)
	#redis_conn.rpush(accident_route_id, (accident_occur_clear, user_req))
	#route_inc_id = "route_incident_num%d" % accident_route_num
	#inc_key_if_exists(route_inc_id)
	redis_conn.rpush("raw_input", ' '.join([accident_occur_clear, user_req[0], user_req[1]]))

def db_user_register(redis_conn, user_req):
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

#user_accident = directKafkaStream_accident.map(lambda (k,v): json.loads(v)).map(lambda(v): (int(v['incident_route_num']), (int(v['incident_loc']), v['ts'])))

user_accident = directKafkaStream_accident.map(lambda (k,v): json.loads(v)).map(lambda(v): (v['incident_route_num'], ' '.join([v['incident_loc'], v['ts'], v['occur_clear'], v['accident_duration']])))
#user_accident.pprint()
user_accident.foreachRDD(lambda rdd: rdd.foreachPartition(processAccident))
#user_accident.mapPartitions(processAccident)

user_pos = directKafkaStream_pos.map(lambda (k,v): json.loads(v)).map((lambda(v):(v['user_route_num'], ' '.join([v['user_loc'], v['user_id'], v['ts']]))))
#user_pos.pprint(num=1)
user_keyed = user_pos.combineByKey(lambda v: v, lambda a, b: a + ' ' + b, lambda a, b : a + ' ' + b)
#user_keyed = user_pos.reduceByKey(lambda a, b : a + b)
user_keyed.foreachRDD(lambda rdd: rdd.foreachPartition(processUserPos))
#user_keyed.mapPartitions(processUserPos)
#user_keyed.pprint()

stream.start()
stream.awaitTermination()
