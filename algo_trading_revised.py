from datetime import timedelta
from statistics import mean
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
import time
from numpy import searchsorted

if __name__ == "__main__":
    spark = SparkSession\
    .builder.appName("AlgoTrading").config('spark.sql.codegen.wholeStage', 'false').getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    #spark.sparkContext.setLogLevel("TRACE")

    spark.conf.set("spark.sql.session.timeZone", "UTC")

    
    schema = StructType([StructField("Date", LongType(), True),\
        StructField("Open", DoubleType(), True),\
        StructField("High", DoubleType(), True),\
        StructField("Low", DoubleType(), True),\
        StructField("Close",DoubleType(), True),\
        StructField("Volume", DoubleType(), True),\
        StructField("Name", StringType(), True)])

    Df1 = spark\
        .readStream\
        .format("csv")\
        .option("maxFilesPerTrigger", 2)\
        .option("header", True)\
        .option("path", "./data/spark_stream_dir")\
        .schema(schema)\
        .load()

    #withTime = Df1.withColumn("DateTime", F.unix_timestamp('Date', 'yyyy-MM-dd').cast('timestamp')) # add a timestamp column because streaming window aggregation requires timestamp type
    withTime = Df1.withColumn('DateTime', F.from_unixtime('Date').cast('timestamp'))

    def ma20(past50days):
        #past20days: [struct['DateTime': Timestamp, 'Close': Double, 'window.end': Timestamp]]
        cutoff = past50days[0][2] - timedelta(days=21)

        filtered = []
        for i in range(len(past50days)):
            if past50days[i][0] > cutoff:
                filtered.append(past50days[i][1])
        if not filtered:
            return None

        #times = [row[0] for row in past50days]
        #i = searchsorted(times, cutoff, side='left')
        #if i == len(past50days):
        #    return None
        #filtered = [row[1] for row in past50days[i:]]

        return mean(filtered)
    
    ma20udf = F.udf(ma20, DoubleType())

    
    movingAverage = withTime.withWatermark('DateTime', "1 second")\
        .groupBy(F.col('Name'), F.window(F.col('DateTime'), "50 days", "1 day"))\
                .agg(ma20udf(F.collect_list(F.struct("DateTime", "Close", "window.end"))).alias("ma_20_days"), \
                    F.mean("Close").alias("ma_50_days"))\
                    .select('Name', F.to_date('window.end').alias('ResultDate'), 'ma_20_days', 'ma_50_days')\
                        .orderBy('ResultDate')

    #   
    # if no watermark is specified, all intermediate state will be stored in memory and all out of order (delayed) events will be handled until memory runs out.
    
    # if a watermark is specified, events that are delayed past the watermark will be dropped. Whether an event is delayed past the watermark
    # depends on the window it belongs to. If the (maximum eventTime seen by the stream engine) - (watermark) > (endTime of the window the event belongs to),
    # then the event is dropped for that window. Since the event can belong to multiple windows in the case of sliding windows (as opposed to 
    # tumbling windows), it may still be accounted for in a later window.
    
    # ORDERBY CAN ONLY BE USED IN COMPLETE OUTPUT MODE



    # generates BUY signal if 10-day moving average is greater than 20-day moving average
    result = movingAverage.withColumn("Buy", F.col('ma_20_days') > F.col('ma_50_days'))


    startTime = time.time()
    query = result.writeStream.outputMode('complete').option('numRows', 600).format("console").trigger(once=True).start().awaitTermination() 
    #query = result.writeStream.outputMode('complete').option('numRows', 600).format("console").start().awaitTermination() 
    endTime = time.time()
    print(endTime - startTime) 
 
    