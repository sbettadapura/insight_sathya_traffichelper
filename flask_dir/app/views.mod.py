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


def get_redis_stats_global():
	r = redis.Redis(host = sys.argv[1], port = 6379, password = 'noredishackers')
	d = [[(k, r.get(k)) for k in r.keys(keys)] for keys in ("curr:routes_affected", "curr:users_affected_cnt", "curr_incidents_reported_cnt", "curr_incidents_cleard_cnt")]
	return reduce(add, d)

def get_redis_stats_byroute(route_num):
	accident_route_id = "curr:incident_route%s" % route_num
	users_on_route = "curr:user_on_route_cnt_%s" % route_num
	users_affected_on_route = "curr:route_incident_users_%s" % route_num
	route_inc_occurence_id = "curr:route_inc_occur_cnt%s" % route_num

	r = redis.Redis(host = sys.argv[1], port = 6379, password = 'noredishackers')
	incidents = r.smembers(accident_route_id)
	if incidents is None:
		print "No incidents on Route %s", route_num
		return

	d = [[(k, r.get(k)) for k in r.keys(keys)] for keys in (route_inc_occurence_id, users_on_route, users_affected_on_route)]
	return reduce(add, d)

@app.route('/api/curr/json/global')
def current_query():
	d = get_redis_stats_global()
	return jsonify(d)


@app.route('/api/curr/global')
def global_stats(chartID = 'chart_ID', chart_type = 'column', chart_height = 350):
	d = get_redis_stats_global()
	dx = [{"name": x[0],"data": [int(x[1])]} for x in d]
	chart = {"renderTo": chartID, "type": chart_type, "height": chart_height,}
	#series = [{"name": 'Label1', "data": [1,2,3]}, {"name": 'Label2', "data": [4, 5, 6]}]
	series = dx
	title = {"text": 'Commute Stats'}
	xAxis = {"categories": ['Calls', 'Size', 'Race Conditions']}
	yAxis = {"title": {"text": 'Current Traffic Stats'}}
	return render_template('index.html', chartID=chartID, chart=chart, series=series, title=title, xAxis=xAxis, yAxis=yAxis)

@app.route('/api/curr/byroute/<route_num>')
def byroute_stats(chartID = 'chart_ID', chart_type = 'column', chart_height = 350):
	d = get_redis_stats_byroute(route_num)
	dx = [{"name": x[0],"data": [int(x[1])]} for x in d]
	chart = {"renderTo": chartID, "type": chart_type, "height": chart_height,}
	#series = [{"name": 'Label1', "data": [1,2,3]}, {"name": 'Label2', "data": [4, 5, 6]}]
	series = dx
	title = {"text": 'Commute Stats'}
	xAxis = {"categories": ['Calls', 'Size', 'Race Conditions']}
	yAxis = {"title": {"text": 'Current Traffic Stats'}}
	return render_template('index.html', chartID=chartID, chart=chart, series=series, title=title, xAxis=xAxis, yAxis=yAxis)
