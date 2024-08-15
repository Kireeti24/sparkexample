import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Spark App").getOrCreate()

# Employee earning more than their manager

def main():
    data = [(1, 'Jhon', 6000, 4), (2, 'Kevin', 11000, 4), (3, 'Bob', 8000, 5), (4, 'Laura', 9000, None),
            (5, 'Sarah', 10000, None)]
    schema = ['ID', 'Name', 'Salary', 'ManagerID']
    df = spark.createDataFrame(data, schema)
    df.show()
    df = df.alias('df1').join(df.alias('df2'), col('df1.managerID') == col('df2.ID'), 'inner').select(col('df1.Name').alias('Employee_name')).filter('df1.salary > df2.salary')
    df.show()

if __name__ == "__main__":
    main()
