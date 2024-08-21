import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Spark App").getOrCreate()


def main():
    productdata = [(1, 'A', 1000), (2, 'B', 400), (3, 'C', 500)]
    productSchema = ['pid', 'pname', 'price']
    df = spark.createDataFrame(productdata, productSchema)
    df.printSchema()
    df.show()

    transactiondata = [(1, '2024-02-01', 2, 2000), (1, '2024-03-01', 4, 4000),
                       (1, '2024-03-15', 2, 2000), (3, '2024-04-24', 3, 1500), (3, '2024-05-16', 5, 2500)]
    transactionSchema = ['pid', 'sold_date', 'qty', 'amount']
    df1 = spark.createDataFrame(transactiondata, transactionSchema)
    df1.printSchema()
    df1.show()

    df2 = df.alias('df').join(df1.alias('df2'), df['pid'] == df1['pid'], 'left').select(col('df.pid'), col('df.pname'),
                                                                                        year(col('sold_date')).alias(
                                                                                            'year'),
                                                                                        month(col('sold_date')).alias(
                                                                                            'Month'),
                                                                                        col('amount').alias(
                                                                                            'Total_sales'))
    df2 = df2.groupBy('df.pid', 'df.pname', 'year', 'Month').agg(
        coalesce(sum('Total_sales'), lit(0)).alias('total_sales')).orderBy('df.pid')
    df2.show()


if __name__ == "__main__":
    main()
