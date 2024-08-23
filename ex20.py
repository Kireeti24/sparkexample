import os
import sys

from pyspark.sql.window import Window

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Exercise 20").getOrCreate()


def main():
    data = [('2019-01-01', 'success'), ('2019-01-02', 'success'), ('2019-01-03', 'success'), ('2019-01-04', 'fail'),
            ('2019-01-05', 'fail'), ('2019-01-06', 'success')]
    schema = ['Date_value', 'Status']
    df = spark.createDataFrame(data, schema)
    df.show()

    df = df.withColumn("prev_status", lag("Status").over(Window.orderBy("Date_value")))

    # Identify changes in the status
    df = df.withColumn("status_change", when(col("Status") != col("prev_status"), lit(1)).otherwise(lit(0)))
    df.show()

    df = df.withColumn("streak_id", sum("status_change").over(Window.orderBy("Date_value")))
    df.show()

    df = df.groupBy('streak_id', 'status').agg(first('date_value'), last('date_value')).drop('streak_id')
    df.show()


if __name__ == "__main__":
    main()
