import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Spark App").getOrCreate()


def main():
    data = [('1234567812345678',), ('2345678923456789',), ('3456789034567890',)]
    schema = ['Credit_cards']
    df = spark.createDataFrame(data, schema)
    df.show()

    df = df.withColumn('Credit_card_regex_replace',
                       regexp_replace(substring('Credit_cards', 0, 11), '[0123456789]', '*'))

    df = df.withColumn('credit_card_', substring('Credit_cards', -4, 4))

    df = df.withColumn('final_Credit_cards',
                       concat(col('Credit_card_regex_replace'), col('Credit_card_')).cast('string')).drop(
        'credit_card_', 'Credit_card_regex_replace')
    df.show()


if __name__ == "__main__":
    main()
