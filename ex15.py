import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Spark App").getOrCreate()


def main():
    data = [('India', 'SL', 'India'), ('SL', 'Aus', 'Aus'), ('SA', 'Eng', 'Eng'), ('Eng', 'NZ', 'NZ'),
            ('Aus', 'India', 'India')]
    schema = ['Team1', 'Team2', 'Team_won']
    df = spark.createDataFrame(data, schema)
    df.show()

    df = df.select(col('Team1'), (when(col('Team1') == col('Team_won'), 1).otherwise(0)).alias('win_flag')).unionAll(
        df.select(col('Team2'), (when(col('Team2') == col('Team_won'), 1).otherwise(0)).alias('Win_flag')))
    df.show()

    df = df.groupBy('Team1').agg(count('Team1').alias('No_of_matches_played'), sum('win_flag').alias('total_wins')).withColumn('Losses', col('No_of_matches_played') - col('total_wins'))
    df.show()


if __name__ == "__main__":
    main()
