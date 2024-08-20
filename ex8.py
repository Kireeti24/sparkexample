import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Spark App").getOrCreate()


class Covid:

    def data(self):
        data = [
            (20124, '2020-01-10'),
            (40133, '2020-01-15'),
            (65005, '2020-01-20'),
            (30005, '2020-02-08'),
            (35015, '2020-02-19'),
            (15015, '2020-03-03'),
            (35035, '2020-03-10'),
            (49099, '2020-03-14'),
            (84045, '2020-03-20'),
            (100106, '2020-03-31'),
            (17015, '2020-04-04'),
            (36035, '2020-04-11'),
            (50099, '2020-04-13'),
            (87045, '2020-04-22'),
            (101101, '2020-04-30'),
            (40015, '2020-05-01'),
            (54035, '2020-05-09'),
            (71099, '2020-05-14'),
            (82045, '2020-05-21'),
            (90103, '2020-05-25'),
            (99103, '2020-05-31'),
            (11015, '2020-06-03'),
            (28035, '2020-06-10'),
            (38099, '2020-06-14'),
            (45045, '2020-06-20'),
            (36033, '2020-07-09'),
            (40011, '2020-07-23'),
            (25001, '2020-08-12'),
            (29990, '2020-08-26'),
            (20112, '2020-09-04'),
            (43991, '2020-09-18'),
            (51002, '2020-09-29'),
            (26587, '2020-10-25'),
            (11000, '2020-11-07'),
            (35002, '2020-11-16'),
            (56010, '2020-11-28'),
            (15099, '2020-12-02'),
            (38042, '2020-12-11'),
            (73030, '2020-12-26')
        ]

        schema = "cases_reported int , dates string"
        df = spark.createDataFrame(data=data, schema=schema)
        df.show()
        return df

    def transform(self, df):
        df = df.withColumn('Month', month(col('dates'))).groupBy('Month').agg(sum('cases_reported').alias('cases_reported')).withColumn('Percentage', (col('cases_reported')/df.agg(sum("cases_reported")).collect()[0][0])*100).drop('dates')
        return df


obj = Covid()
input_df = obj.data()
output_df = obj.transform(input_df)
output_df.show()
