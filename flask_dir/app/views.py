#!/usr/bin/env python
from app import app
from flask import jsonify
from flask import Flask, render_template
import redis
import sys
from operator import add
import psycopg2
import datetime
import os

#HOST = os.environ['REDSHIFT_HOST']
PORT = 5439 # redshift default
#USER = os.environ['REDSHIFT_USER']
#PASSWORD = os.environ['REDSHIFT_PASSWD']
#DATABASE = os.environ['REDSHIFT_DATABASE']
HOST='sathyared.cniqeoxrupxt.us-west-2.redshift.amazonaws.com'
USER='sbettadapura'
PASSWORD='Pach655!'
DATABASE='trhist'

@app.route('/')
@app.route('/index')
def index():
  return "Welcome to Traffic Helper!"

def rs_connection():
	return psycopg2.connect(host=HOST, port=PORT, user=USER, password=PASSWORD, database=DATABASE)


def get_redshift_users():
	rs_conn = rs_connection()
	cursor = rs_conn.cursor()

	cursor.execute("CREATE TABLE hist_day_time AS SELECT to_char(tod, 'YYYY-MM-DD') as idate, to_char(tod, 'HH:MM:SS') AS itime, route_num, location, users_total, users_affceted, duration FROM hist_traffic")

	cursor.execute("SELECT idate, SUM(users_total), SUM(users_affceted) FROM hist_day_time GROUP BY idate")
	idates = []
	sut = []
	sua = []
	for i in cursor.fetchall():
		idate, sum_users_total, sum_users_affected = i
		idates.append(idate)
		sut.append(int(sum_users_total))
		sua.append(int(sum_users_affected))
	return idates, sut, sua

def get_redis_user_stats_global():
	r = redis.Redis(host = sys.argv[1], port = 6379, password = 'noredishackers')
	d = [[(k, r.get(k)) for k in r.keys(keys)] for keys in ("users_total", "users_affected")]
	return reduce(add, d)

def get_redis_user_stats_byroute(route_num):
	users_on_route = "users_total_on_route_%s" % route_num
	users_affected_on_route = "users_affected_on_route_%s" % route_num

	r = redis.Redis(host = sys.argv[1], port = 6379, password = 'noredishackers')
	d = [[(k, r.get(k)) for k in r.keys(keys)] for keys in (users_on_route, users_affected_on_route)]
	return reduce(add, d)

def get_redis_incident_stats():
	incidents_reported = "incidents_reported"
	incidents_cleared = "incidents_cleared"

	r = redis.Redis(host = sys.argv[1], port = 6379, password = 'noredishackers')
	d = [[(k, r.get(k)) for k in r.keys(keys)] for keys in (incidents_reported, incidents_cleared)]
	return reduce(add, d)

@app.route('/api/curr/json/global')
def current_query():
	d = get_redis_stats_global()
	return jsonify(d)


@app.route('/api/curr/users')
def global_stats(chartID = 'chart_ID', chart_type = 'column', chart_height = 350):
	d = get_redis_user_stats_global()
	dx = [{"name": x[0],"data": [int(x[1])]} for x in d]
	chart = {"renderTo": chartID, "type": chart_type, "height": chart_height,}
	series = dx
	title = {"text": 'Current Traffic Stats'}
	xAxis = {"categories": ['Users']}
	yAxis = {"title": {"text": 'Number of Users'}}
	return render_template('index.html', chartID=chartID, chart=chart, series=series, title=title, xAxis=xAxis, yAxis=yAxis)

@app.route('/api/curr/incidents')
def incidents_stats():
	chartID = 'chart_ID'
	chart_type = 'column'
	chart_height = 350
	d = get_redis_incident_stats()
	dx = [{"name": x[0],"data": [int(x[1])]} for x in d]
	chart = {"renderTo": chartID, "type": chart_type, "height": chart_height,}
	series = dx
	title = {"text": 'Curent Traffic Stats'}
	xAxis = {"categories": ['Incidents']}
	yAxis = {"title": {"text": 'Number of Incidents'}}
	return render_template('index.html', chartID=chartID, chart=chart, series=series, title=title, xAxis=xAxis, yAxis=yAxis)
@app.route('/api/curr/<route_num>')
def byroute_stats(route_num):
	chartID = 'chart_ID'
	chart_type = 'column'
	chart_height = 350
	d = get_redis_user_stats_byroute(route_num)
	dx = [{"name": x[0],"data": [int(x[1])]} for x in d]
	chart = {"renderTo": chartID, "type": chart_type, "height": chart_height,}
	series = dx
	title = {"text": 'Current Traffic Stats'}
	xAxis = {"categories": ['Users']}
	yAxis = {"title": {"text": 'Number of Users'}}
	return render_template('index.html', chartID=chartID, chart=chart, series=series, title=title, xAxis=xAxis, yAxis=yAxis)

@app.route('/api/conditions/<route_num>')
def road_condition(route_num):

	x = "incident_on_route_%s" % route_num
	r = redis.Redis(host = sys.argv[1], port = 6379, password = 'noredishackers')
	loc = r.get(x)
	if loc is None:
		roadtext = 'No incidents on Route %s' % route_num
	else:
		roadtext = 'Incident(s) on Route %s at location %s' % (route_num, loc)

  	roadcond = { 'roadtext': roadtext } # fake user
  	return render_template("roadcond.html", title = 'Road Condition', roadcond = roadcond)

@app.route('/api/hist/users')
def global_hist_users(chartID = 'chart_ID', chart_type = 'column', chart_height = 350):
	idates, sut, sua = get_redshift_users()
	dx1 = {"name": "users_total","data": [x for x in sut]}
	dx2 = {"name": "users_affected","data": [x for x in sua]}
	chart = {"renderTo": chartID, "type": chart_type, "height": chart_height,}
	#series = [{"name": 'Label1', "data": [1,2,3]}, {"name": 'Label2', "data": [4, 5, 6]}]
	series = [dx1, dx2]
	title = {"text": 'Historical Traffic Stats'}
	xAxis = {"categories": idates}
	yAxis = {"title": {"text": 'Number of Users'}}
	return render_template('index.html', chartID=chartID, chart=chart, series=series, title=title, xAxis=xAxis, yAxis=yAxis)
