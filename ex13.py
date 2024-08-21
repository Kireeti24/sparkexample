import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import *

spark = SparkSession.builder.getOrCreate()


def main():
    data = [(4, 'R'), (2, 'S'), (3, "Ra")]
    schema = StructType([StructField("Count", IntegerType(), True),
                         StructField('Str', StringType(), True)])
    df = spark.createDataFrame(data, schema)
    df.show()
    df.withColumn('cmb_str', expr('repeat(Str,Count)')).show()


if __name__ == "__main__":
    main()
