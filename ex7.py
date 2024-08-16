import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Spark App").getOrCreate()

def main():
    data = [(1, 'I love to play cricket',), (2, 'I am into motorbiking',), (3, 'What do you like',)]
    schema = ["id", "message"]
    df = spark.createDataFrame(data=data, schema=schema)
    df.show(truncate=False)
    df = df.withColumn('Consonant', regexp_replace(col('message'), '[aeiouAEIOU ]', '')).withColumn('Consonant_count', length(col('Consonant')))
    df.show(truncate=False)


if __name__ == "__main__":
    main()