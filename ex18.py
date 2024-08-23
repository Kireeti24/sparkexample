import os
import sys

from pyspark.sql.functions import *

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Python Spark").getOrCreate()


def main():
    df = spark.createDataFrame([(1,)], ['dummy'])
    df = df.withColumn('current_date', current_date())
    df.show()
    n = int(input("Please enter nth occurence of sunday from today: "))
    df = df.withColumn(f'Occurence_of_{n}th_sunday', date_add('current_date', n))
    df.show()


if __name__ == "__main__":
    main()
