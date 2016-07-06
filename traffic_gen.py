#!/usr/bin/python
"""
	Generate traffic input. Generate user loation update, send it out on
	Kafka topi "user_pos". Incident occurence and clearence are generated at
	about 2% each compaed to user location update. These messages are sent
	to Kafka topic "user_accident". Both are JSON messages.	
"""
from kafka import KafkaProducer, KeyedProducer
from kafka.errors import KafkaError
import datetime
from random import randrange
import sys
import time
import json
NUM_PARTITIONS = 18
NUM_USERS = 100000
NUM_ROUTES = 100
MAX_ROUTE_LEN = 23
TS_FMT = '%Y-%m-%d %H:%M:%S'
route_dict = dict()

servers = ','.join([ip + ":9092" for ip in sys.argv[1:]])
producer = KafkaProducer(bootstrap_servers = servers, value_serializer=lambda m: json.dumps(m).encode('ascii'))

def gen_route_info():
	rt_start = 0
	for i in range(NUM_ROUTES):
		rt_len = randrange(MAX_ROUTE_LEN/2, MAX_ROUTE_LEN)
		route_dict[i] = (rt_start, rt_start + rt_len)
		rt_start += rt_len

def gen_update_pos():
	ts = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
	rand_route_num = randrange(NUM_ROUTES)
	start,end = list(route_dict[rand_route_num])
	rand_loc = randrange(start, end)
	user_id = "user%d" % randrange(NUM_USERS)
	user_data = {"user_route_num" : str(rand_route_num), "user_loc": str(rand_loc), "user_id" : user_id, "ts": ts}
	kafka_key = rand_route_num % NUM_PARTITIONS
	return (kafka_key, user_data)


def gen_update_accident():
	ts = datetime.datetime.now().strftime(TS_FMT)
	rand_route_num = randrange(NUM_ROUTES)
	start,end = list(route_dict[rand_route_num])
	rand_loc = randrange(start, end)
	user_data = {"incident_route_num" : str(rand_route_num), "incident_loc" : str(rand_loc), "ts": ts, "occur_clear" : str(1), "accident_duration": str(0)} 
	kafka_key = rand_route_num % NUM_PARTITIONS
	return (kafka_key, user_data)

def gen_clear_accident():
	ts = datetime.datetime.now().strftime(TS_FMT)
	rand_route_num = randrange(NUM_ROUTES)
	start,end = list(route_dict[rand_route_num])
	rand_loc = randrange(start, end)
	user_data = {"incident_route_num" : str(rand_route_num), "incident_loc" : str(rand_loc), "ts": ts, "occur_clear" : str(0), "accident_duration": str(randrange(5, 15))} 
	kafka_key = rand_route_num % NUM_PARTITIONS
	return (kafka_key, user_data)

def gen_user_traffic():
	x = randrange(100)
	if x >= 0 and x < 2:
		kafka_key, data = gen_update_accident()
		return ("user_accident", data, kafka_key)
	elif x >=2 and x < 4:
		kafka_key, data = gen_clear_accident()
		return ("user_accident", data, kafka_key)
	elif x >= 4 and x < 100:
		kafka_key, data = gen_update_pos()
		return ("user_pos", data, kafka_key)

def trigger_gen():
	topic, d, kafka_key = gen_user_traffic()
	try:

		#print topic, d, kafka_key
		future = producer.send(topic, d)
	except KafkaError:
		print "Kafka error"
		pass
if __name__ == "__main__":
	gen_route_info()
	while True:
		trigger_gen()
