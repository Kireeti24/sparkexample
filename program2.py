import os,sys
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import *
from pyspark.sql.window import Window

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
spark = SparkSession.builder.master('local[*]').getOrCreate()

data = [(datetime(2021,5,1,11,00,00),'u1'),(datetime(2021,5,1,13,13,00),'u1'),(datetime(2021,5,1,15,00,00),'u2'),(datetime(2021,5,1,11,25,00),'u1'),(datetime(2021,5,1,15,15,00),'u2'),(datetime(2021,5,1,2,13,00),'u3'),(datetime(2021,5,3,2,15,00),'u4'),(datetime(2021,5,2,11,45,00),'u1'),(datetime(2021,5,2,11,00,00),'u3'),(datetime(2021,5,3,12,15,00),'u3'),(datetime(2021,5,3,11,00,00),'u4'),(datetime(2021,5,3,21,00,00),'u4'),(datetime(2021,5,4,19,00,00),'u2'),(datetime(2021,5,4,9,00,00),'u3'),(datetime(2021,5,1,8,15,00),'u1')]
schema = ['timestamp', 'user_id']
df = spark.createDataFrame(data, schema)
df.show()

window_spec = Window.partitionBy('user_id').orderBy('timestamp')

df = df.withColumn('prev_click_time',lag('timestamp').over(window_spec))
df.show()

df = df.withColumn('timediff',unix_timestamp('timestamp') - unix_timestamp('prev_click_time'))
df.show()

inactivity_timer = 60 * 30
max_session_timeout = 60 * 60 * 2

df = df.withColumn('session_id',when(
    (col('timediff') > inactivity_timer) | (col('timediff') > max_session_timeout) | (col('timediff').isNull()),1).
                   otherwise(0))
df.show()

df = df.withColumn('session_id',sum('session_id').over(window_spec))
df.show()

df.select('timestamp','user_id','session_id').show()


