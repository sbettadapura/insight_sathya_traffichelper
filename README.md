# insight_sathya

Synthesize traffic updates from a network consisting of 100 roads of arbitrary
size.

Input is generated by running traffic_gen.py. 
The input that's geerated is processed by running traffic_process.py via
a bash script called mypark.sh. myspark.sh does "spark-submit" with the needed
packages. traffic_process.py populates data in a Redis database.

Ths data is analyzed by a Python program offine_process.py. You run the 
generator, processor and offline processors all at the same time.

In the directory flask_dir, run "run.py" for UI.
