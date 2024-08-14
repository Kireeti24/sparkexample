import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import when, col, lag, sum, first, last, to_date
from pyspark.sql.window import Window

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
spark = SparkSession.builder.getOrCreate()


class Data:
    def Createdata(self):
        data = [('01-06-2020', 'Won'), ('02-06-2020', 'Won'), ('03-06-2020', 'Won'), ('04-06-2020', 'Lost'),
                ('05-06-2020', 'Lost'), ('06-06-2020', 'Lost'), ('07-06-2020', 'Won')]
        schema = ['event_date', 'Event_status']
        return spark.createDataFrame(data=data, schema=schema)

    def transform(self, df):
        df.repartition('event_status')
        df = df.withColumn('event_date', to_date('event_date', 'dd-MM-yyyy'))
        df = df.withColumn('event_change',
                           when(col('event_status') != lag('event_status').over(Window.orderBy('event_date')),
                                1).otherwise(0))
        df = df.withColumn('event_change', sum('event_change').over(Window.orderBy('event_date')))
        df.groupBy('event_status', 'event_change').agg(first('event_date').alias('start_date'),
                                                       last('event_date').alias('end_date')).drop('event_change').show()


ob = Data()
input_df = ob.Createdata()
transform = ob.transform(input_df)