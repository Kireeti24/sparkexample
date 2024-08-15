import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("SparkApp").getOrCreate()


class College:
    def data(self):
        users_data = [(1, 'Alice', 'HR', 60000), (2, 'Bob', 'HR', 50000), (3, 'Charlie', 'Finance', 70000),
                      (4, 'David', 'Finance', 75000), (5, 'Eve', 'Engineering', 90000),
                      (6, 'Frank', 'Engineering', 93000), (7, 'Grace', 'HR', 45000), (8, 'Hank', 'Engineering', 98000),
                      (9, 'Ivy', 'Finance', 63000)]
        schema = ['Employee_Id', 'Employee_Name', 'Department', 'Salary']
        return spark.createDataFrame(users_data, schema)

    def transform(self, df):
        df = (df.withColumn('Rank', dense_rank().over(Window.partitionBy('Department')
                                                      .orderBy(col('salary').desc()))).withColumn('avg_salary',
                                                                                                  avg('Salary')
                                                                                                  .over(
                                                                                                      Window.partitionBy(
                                                                                                          'Department').orderBy(
                                                                                                          'Salary')))
              .filter('Rank == 1').drop('Rank', 'Employee_ID'))
        return df


ob = College()
inputDf = ob.data()
outputDf = ob.transform(inputDf)
outputDf.show()
