import os
import sys

from pyspark.sql.types import StructType

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, count, sum

spark = SparkSession.builder.appName('Youtubeex2').getOrCreate()


def main():
    data = [('10', '20', 58), ('20', '10', 12), ('10', '30', 20), ('30', '40', 100), ('30', '40', 200),
            ('30', '40', 200), ('40', '30', 200)]
    schema = ['from_id', 'to_id', 'duration']
    df = spark.createDataFrame(data, schema)
    df.show()

    df = df.withColumn('person1',
                       when(col('from_id') > col('to_id'), col('from_Id')).otherwise(col('to_id'))).withColumn(
        'person2', when(col('from_id') > col('to_id'), col('to_id')).otherwise(col('from_id'))).drop('from_id', 'to_id')
    df.show()
    df = df.groupBy('person1', 'person2').agg(sum('duration').alias('duration(in minutes)'), count('person1').alias('call_count'))
    df.show()


if __name__ == '__main__':
    main()
