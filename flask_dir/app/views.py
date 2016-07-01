from app import app
from flask import jsonify
from flask import Flask, render_template
import redis
import sys
from operator import add

@app.route('/')

@app.route('/index')

def index():

  return "Welcome to Traffic Helper!"


def get_redis_user_stats_global():
	r = redis.Redis(host = sys.argv[1], port = 6379, password = 'noredishackers')
	#d = [[(k, r.get(k)) for k in r.keys(keys)] for keys in ("curr:routes_affected", "curr:users_affected_cnt", "curr:incidents_reported_cnt", "curr:incidents_cleared_cnt")]
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
	#d = [[(k, r.get(k)) for k in r.keys(keys)] for keys in (incidents_reported, incidents_reported_on_route, incidents_cleared, incidents_cleared_on_route)]
	#d = [[(k, r.get(k)) for k in r.keys(keys)] for keys in (incidents_reported_on_route, incidents_cleared, incidents_cleared_on_route)]
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
	#series = [{"name": 'Label1', "data": [1,2,3]}, {"name": 'Label2', "data": [4, 5, 6]}]
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
	#series = [{"name": 'Label1', "data": [1,2,3]}, {"name": 'Label2', "data": [4, 5, 6]}]
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
	#series = [{"name": 'Label1', "data": [1,2,3]}, {"name": 'Label2', "data": [4, 5, 6]}]
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
		roadtext = 'Incidents on Route %s at location %s' % (route_num, loc)

  	roadcond = { 'roadtext': roadtext } # fake user
  	return render_template("roadcond.html", title = 'Road Condition', roadcond = roadcond)
