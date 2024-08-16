import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Spark App").getOrCreate()


def main():
    data = [(1, 5), (2, 6), (3, 5), (3, 6), (1, 6)]
    schema = ['Customer_id', 'Product_key']
    df = spark.createDataFrame(data, schema)
    df.show()
    product_data = [(5,), (6,)]
    df1 = spark.createDataFrame(product_data, schema=['Product_key'])
    df1.show()
    df.alias('df').join(df1.alias('df1'), col('df1.Product_key') == col('df.Product_key'), 'inner').show()


if __name__ == "__main__":
    main()
