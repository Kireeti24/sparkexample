import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("Python Spark").getOrCreate()


def main():
    data = [(53151, 'deposit', 178, '2022-07-08'), (29776, 'withdrawal', 25, '2022-07-08'),
            (16461, 'withdrawal', 45, '2022-07-08'), (19153, 'deposit', 65, '2022-07-10'),
            (77134, 'deposit', 32, '2022-07-10')]
    schema = ['transaction_id', 'type', 'amount', 'transaction_date']
    df = spark.createDataFrame(data, schema)
    df.show()

    df = df.withColumn('rn', row_number().over(
        Window.partitionBy('transaction_date').orderBy('transaction_date'))).withColumn('runnimg_total', sum(when(
        col('type') == 'deposit', col('amount')).otherwise(-col('amount'))).over(
        Window.orderBy('transaction_date', 'rn')))
    df.show()


if __name__ == "__main__":
    main()
