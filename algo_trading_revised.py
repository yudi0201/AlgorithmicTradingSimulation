from datetime import timedelta
from statistics import mean
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
import time

if __name__ == "__main__":
    spark = SparkSession\
    .builder.appName("AlgoTrading").getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    #spark.sparkContext.setLogLevel("TRACE")

    
    schema = StructType([StructField("Date", DateType(), True),\
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
        .option("path", "./data/streams")\
        .schema(schema)\
        .load()

    withTime = Df1.withColumn("DateTime", F.unix_timestamp('Date', 'yyyy-MM-dd').cast('timestamp')) # add a timestamp column because streaming window aggregation requires timestamp type


    def ma10(past20days):
        #past20days: [struct['DateTime': Timestamp, 'Close': Double, 'window.end': Timestamp]]
        cutoff = past20days[0][2] - timedelta(days=10)
        filtered = []
        for i in range(len(past20days)):
            if past20days[i][0] > cutoff:
                filtered.append(past20days[i][1])
        if not filtered:
            return None
        return mean(filtered)
    
    ma10udf = F.udf(ma10, DoubleType())

    
    movingAverage = withTime\
        .groupBy(F.col('Name'), F.window(F.col('DateTime'), "20 days", "1 day"))\
                .agg(ma10udf(F.collect_list(F.struct("DateTime", "Close", "window.end"))).alias("ma_10_days"), \
                    F.mean("Close").alias("ma_20_days"))\
                    .select('Name', F.to_date('window.end').alias('ResultDate'), 'ma_10_days', 'ma_20_days')\
                        .orderBy('ResultDate')


    # generates BUY signal if 10-day moving average is greater than 20-day moving average
    result = movingAverage.withColumn("Buy", F.col('ma_10_days') > F.col('ma_20_days'))


    startTime = time.time()
    query = result.writeStream.outputMode('complete').option('numRows', 600).format("console").start().awaitTermination() 
    endTime = time.time()
    print(endTime - startTime) 
 
    