import psycopg2
import os
import datetime
TS_FMT = '%Y-%m-%d %H:%M:%S'
HOST = os.environ['REDSHIFT_HOST']
PORT = 5439 # redshift default
USER = os.environ['REDSHIFT_USER']
PASSWORD = os.environ['REDSHIFT_PASSWD']
DATABASE = os.environ['REDSHIFT_DATABASE']

def db_connection():
	conn = psycopg2.connect(
	host=HOST,
	port=PORT,
	user=USER,
	password=PASSWORD,
	database=DATABASE,
    )

	return conn
#cursor.execute('INSERT INTO %s (day, elapsed_time, net_time, length, average_speed, geometry) VALUES (%s, %s, %s, %s, %s, %s)', (escaped_name, day, time_length, time_length_net, length_km, avg_speed, myLine_ppy))
#cursor.execute("CREATE TABLE test (id serial PRIMARY KEY, num integer, data varchar);")
#cursor.execute("INSERT INTO test (num, data) VALUES (%s, %s)",(100, "abc'def"))

#create_query = ('CREATE TABLE def (route_id integer, start integer, ts timestamp)')
#create_query = ('CREATE TABLE hist_traffic (tod datetime,  route_num integer, location integer, users_total integer, users_affceted integer, duration integer)')

conn = db_connection()
try:
	cursor = conn.cursor()
	#cursor.execute(create_query)
	cursor.execute("SELECT * from hist_traffic")
	for i in cursor.fetchall():
		print i
finally:
	conn.close()
