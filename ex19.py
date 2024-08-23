import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Python Spark").getOrCreate()


def main():
    data = [('1', '1', '10', '1', 'completed', '2013-10-01'),
            ('2', '2', '11', '1', 'cancelled_by_driver', '2013-10-01'),
            ('3', '3', '12', '6', 'completed', '2013-10-01'),
            ('4', '4', '13', '6', 'cancelled_by_client', '2013-10-01'),
            ('5', '1', '10', '1', 'completed', '2013-10-02'), ('6', '2', '11', '6', 'completed', '2013-10-02'),
            ('7', '3', '12', '6', 'completed', '2013-10-02'), ('8', '2', '12', '12', 'completed', '2013-10-03'),
            ('9', '3', '10', '12', 'completed', '2013-10-03'),
            ('10', '4', '13', '12', 'cancelled_by_driver', '2013-10-03')]
    schema = ['id', 'client_id', 'driver_id', 'city_id', 'status', 'request_at']
    df = spark.createDataFrame(data, schema)
    df.show()

    data2 = [('1', 'No', 'client'), ('2', 'Yes', 'client'),
             ('3', 'No', 'client'),
             ('4', 'No', 'client'),
             ('10', 'No', 'driver'),
             ('11', 'No', 'driver'),
             ('12', 'No', 'driver'),
             ('13', 'No', 'driver')]
    schema_data2 = ['User_id', 'Banned', 'Role']
    df1 = spark.createDataFrame(data2, schema_data2)
    df1.show()

    count1 = count(when((col('status') == 'cancelled_by_driver') | (col('status') == 'cancelled_by_client'), '1'))
    count2 = count(when((col('status') == 'cancelled_by_driver') | (col('status') == 'cancelled_by_client') | (col('status') == 'completed'), 'count'))
    percentage = ((count1.cast('float')/count2.cast('float'))*100).alias('cancelled_percent')

    df_transform = df.groupBy('request_at').agg(count(
        when((col('status') == 'cancelled_by_driver') | (col('status') == 'cancelled_by_driver'), 'status')).alias(
        'Cancelled_count'), count(when(col('status') == 'completed', 'completed')).alias('total_trips'), percentage)
    df_transform.show()


if __name__ == "__main__":
    main()
