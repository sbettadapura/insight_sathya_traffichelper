python traffic_gen.py 52.38.24.171 52.10.97.50 52.26.161.44 50.112.36.69 &
bash myspark.sh traffic_consume.py 4 50.112.50.23 52.38.24.171 52.10.97.50 52.26.161.44 50.112.36.69 >& /tmp/redis.out &
python offline_consume.py ec2-50-112-50-23.us-west-2.compute.amazonaws.com &
