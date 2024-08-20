import os
import sys

from pyspark.sql.types import StructType

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("Spark App").getOrCreate()


def main():
    data = [('Siva', 1, 30000), ('Ravi', 2, 40000), ('Prasad', 1, 50000), ('Sai', 2, 20000), ('Anna', 2, 10000)]
    schema = StructType([StructField('Name', StringType(), True),
                         StructField('Dept_id', IntegerType(), True),
                         StructField('Salary', IntegerType(), True), ])
    df = spark.createDataFrame(data, schema)
    df.show()
    #df = df.withColumn('rnk', dense_rank().over(Window.orderBy('dept_id', 'Salary')))
    df = df.orderBy('dept_id', 'salary')
    df.show()

    df = df.groupBy('dept_id').agg(first('name').alias('min_salary_name'), last('name').alias('max_salary_name'))
    df.show()


if __name__ == "__main__":
    main()
