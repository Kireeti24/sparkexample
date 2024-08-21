import os
import sys

from pyspark.sql.functions import lag
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("Spark App").getOrCreate()


def main():
    data = [(1, 1), (2, 0), (3, 1), (4, 0), (5, 1), (6, 1), (7, 1), (8, 0), (9, 1), (10, 1), (11, 1), (12, 0), (13, 0),
            (14, 0), (15, 1), (16, 1)]
    schema = StructType([StructField("Name", IntegerType(), True),
                         StructField("Age", IntegerType(), True)])
    df = spark.createDataFrame(data, schema)
    df.show()

    df = df.withColumn('lag', lag('Age').over((Window.orderBy('Name')))*col('Age')).withColumn('Lead', lead('Age').over(
        (Window.orderBy('Name')))*col('Age'))
    df.show()

    df = df.filter((col('lag') == 1) | (col('Lead') == 1)).select('Name')
    df.show()


if __name__ == "__main__":
    main()
