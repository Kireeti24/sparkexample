import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("Spark App").getOrCreate()


def main():
    flights_data = [(1, 'Flight1', 'Delhi', 'Hyderabad'),
                    (1, 'Flight2', 'Hyderabad', 'Kochi'),
                    (1, 'Flight3', 'Kochi', 'Mangalore'),
                    (2, 'Flight1', 'Mumbai', 'Ayodhya'),
                    (2, 'Flight2', 'Ayodhya', 'Gorakhpur')
                    ]

    _schema = "cust_id int, flight_id string , origin string , destination string"

    df = spark.createDataFrame(data=flights_data, schema=_schema)
    df.show()

    df = df.groupBy('cust_id').agg(first('origin').alias('origin'), last('destination').alias('destination'))
    df.show()


if __name__ == "__main__":
    main()
