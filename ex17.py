import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("Python Spark").getOrCreate()


def main():
    data = [('A', 'Bangalore', 'A@gmail.com', 1, 'CPU'), ('A', 'Bangalore', 'A1@gmail.com', 1, 'CPU'),
            ('A', 'Bangalore', 'A2@gmail.com', 2, 'DESKTOP'), ('B', 'Bangalore', 'B@gmail.com', 2, 'DESKTOP'),
            ('B', 'Bangalore', 'B1@gmail.com', 2, 'DESKTOP'), ('B', 'Bangalore', 'B2@gmail.com', 1, 'MONITOR')]

    Schema = ['name', 'address', 'email', 'floor', 'resources']
    df = spark.createDataFrame(data, Schema)
    df.show()

    df_final = df.groupBy('name').agg(count(when(col('address') == 'Bangalore', 'total_visits')).alias('total_visits'),
                                concat_ws(',', collect_set(col('resources'))).alias('resources'))
    df_final.show()

    df_floor_count = df.groupBy('name', 'floor').agg(count('*').alias('count')).withColumn('rank', dense_rank().over(Window.partitionBy('name').orderBy(col('count').desc()))).filter('rank == 1')
    df_floor_count.show()

    df_final.join(df_floor_count, on='name', how='inner').withColumnRenamed('floor', 'most_floor_visited ').drop('count', 'rank').show()


if __name__ == "__main__":
    main()
