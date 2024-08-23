import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName('Ex16').getOrCreate()


def main():
    data = [(1, 100, '2022-01-01', 2000), (2, 200, '2022-01-01', 2500), (3, 300, '2022-01-01', 2100),
            (4, 100, '2022-01-02', 2000), (5, 400, '2022-01-02', 2200), (6, 500, '2022-01-02', 2700),
            (7, 100, '2022-01-03', 3000), (8, 400, '2022-01-03', 1000), (9, 600, '2022-01-03', 3000)]
    schema = ['Order_id', 'Customer_id', 'Order_date', 'Price']
    df = spark.createDataFrame(data, schema)
    df.show()

    df = df.withColumn('First_visit_date', min('Order_date').over(Window.partitionBy('customer_id')))
    df.show()
    df = df.withColumn('Visit_type', when(col('order_date') == col('first_visit_date'), 1).otherwise(0))
    df.show()

    df = df.groupBy('order_date').agg(count(when((col('visit_type') == 1), 'First')).alias('First'),
                                      count(when((col('visit_type') == 0), 'Repeat')).alias('Repeat')).orderBy(
        'order_date', desc('First'))
    df.show()


if __name__ == "__main__":
    main()
