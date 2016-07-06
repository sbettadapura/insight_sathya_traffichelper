#!usr/bin/env python
"""
	Process data in Redis key "raw_input". User keep sending location updates
	and occrence and clearence of incidents. We rocess this data by writing	a
	State Machne. Note that as the input is strictly random, we can get
	events out of order such as a clearence for an event that has not even
	been recorded yet. We ignore such input.
	We produce analytics for current queries and write them to Redis. We
	alo write a summary of traffic data to AWS Redshift for historical
	analysis.
"""
import redis
import sys
import os
import time
import psycopg2
from collections import defaultdict

TYPE_CLEAR, TYPE_INCIDENT, TYPE_USERS = range(3)

redis_host = sys.argv[1]

redis_conn = redis.Redis(host = redis_host, port = 6379, password = 'noredishackers')
route_expect = dict((i, TYPE_INCIDENT) for i in range(100))
incident_location = defaultdict(int)

HOST = os.environ['REDSHIFT_HOST']
PORT = 5439 # redshift default
USER = os.environ['REDSHIFT_USER']
PASSWORD = os.environ['REDSHIFT_PASSWD']
DATABASE = os.environ['REDSHIFT_DATABASE']

def redshift_connection():
	conn = psycopg2.connect(
	host=HOST,
	port=PORT,
	user=USER,
	password=PASSWORD,
	database=DATABASE,
    )

	return conn

def historical_stats_duration(accident_tod, accident_route_num, \
			incident_location, accident_duration):
	conn = redshift_connection()
	cursor = conn.cursor()
	x = "users_total_on_route_%d" % accident_route_num
	total_users = int(redis_conn.get(x))
	x = "users_affected_on_route_%d" % accident_route_num
	affected_users = int(redis_conn.get(x))
	cursor.execute("INSERT INTO hist_traffic VALUES(%s, %s, %s, %s, %s, %s)", \
			(accident_tod, accident_route_num, incident_location,\
			total_users, affected_users, accident_duration))
	conn.commit()

def process_users(redis_conn, rec, user_route_num, incident_loc):
	accident_day, accident_tod = None, None
	affected_users = 0
	total_users = len(rec) / 4
	redis_conn.incr("users_total", total_users)
	for i in range(0, len(rec), 4):
		user_loc = int(rec[i])
		user_id = rec[i + 1]
		ts_day = rec[i + 2]
		ts_tod = rec[i + 3]
		if accident_day is None:
			accident_day, accident_tod = ts_day, ts_tod
		if user_loc > incident_loc and incident_loc - user_loc <= 2:
			affected_users += 1

	x = "users_total_on_route_%d" % user_route_num
	redis_conn.incr(x, total_users)
	x = "users_affected_on_route_%d" % user_route_num
	redis_conn.incr(x, affected_users)
	redis_conn.incr("users_affected", affected_users)

while True:
	type_rec = redis_conn.lpop("raw_input")
	if type_rec is None:
		time.sleep(1.0/100)
		continue
	type = int(type_rec.split()[0])
	accident_route_num = int(type_rec.split()[1])
	if type == TYPE_INCIDENT:
		if route_expect[accident_route_num] != TYPE_INCIDENT:
			pass # ignore
		else:
			loc = int(type_rec.split()[2])
			if incident_location[accident_route_num] == 0:
				redis_conn.incr("routes_affected")
			incident_location[accident_route_num] = loc
			redis_conn.incr("incidents_reported")
			x = "incidents_reported_on_route_%d" % accident_route_num
			redis_conn.incr(x)
			x = "incident_on_route_%d" % accident_route_num
			redis_conn.set(x, str(loc))

			route_expect[accident_route_num] = TYPE_USERS
	elif type == TYPE_CLEAR:
		if route_expect[accident_route_num] != TYPE_CLEAR:
			pass # ignore
		else:
			redis_conn.incr("incidents_cleared")
			x = "incidents_cleared_on_route_%d" % accident_route_num
			redis_conn.incr(x)
			accident_duration = int(type_rec.split()[6])
			accident_day, accident_tod = type_rec.split()[3:5]
			accident_ts = ' '.join([accident_day, accident_tod])
			historical_stats_duration(accident_ts, \
				accident_route_num,\
				incident_location[accident_route_num],\
				accident_duration)
			incident_location[accident_route_num] = 0
			x = "incident_on_route_%d" % accident_route_num
			redis_conn.delete(x)
			route_expect[accident_route_num] = TYPE_INCIDENT
	elif type == TYPE_USERS:
		if route_expect[accident_route_num] != TYPE_USERS:
			pass # ignore
		else:
			process_users(redis_conn, type_rec.split()[2:], accident_route_num, \
						incident_location[accident_route_num])
			route_expect[accident_route_num] = TYPE_CLEAR
