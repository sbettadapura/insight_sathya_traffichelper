import redis
import sys
redis_host = sys.argv[1]

r = redis.Redis(host = redis_host, port = 6379, db = 0, password ='noredishackers')
for k in r.keys("*"):
	r.delete(k)
