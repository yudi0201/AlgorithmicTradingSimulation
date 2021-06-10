from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.conf import SparkConf
import time
import pandas as pd

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
    
    Df1.cache()

    withTime = Df1.withColumn("DateTime", unix_timestamp('Date', 'yyyy-MM-dd').cast('timestamp')) # add a timestamp column because streaming window aggregation requires timestamp type

    # User-defined aggregate function that calculates two moving averages, one for the past 10 days, another for the past 20 days
    @pandas_udf("Stock string, ResultDate date, 10_day_MA double, 20_day_MA double", functionType=PandasUDFType.GROUPED_MAP)
    def ma(pdf):
        cutoff_date = pdf["DateTime"].max() - pd.Timedelta(days=10)
        pdf1 = pdf[pdf['DateTime'] > cutoff_date] 
        return pd.DataFrame([[pdf['Name'].iloc[0], pdf['DateTime'].max().date(), pdf1['Close'].mean(), pdf['Close'].mean()]], \
            columns = ['Stock', 'ResultDate', '10_day_MA', '20_day_MA'])

    
    movingAverage = withTime.withWatermark('DateTime', "0.001 seconds")\
        .groupBy(window(col('DateTime'), "20 days", "1 day"), col('Name'))\
                .apply(ma)
                    #.orderBy(col('ResultDate'))

    # generates BUY signal if 10-day moving average is greater than 20-day moving average
    result = movingAverage.withColumn("Buy", col('10_day_MA') > col('20_day_MA'))


    startTime = time.time()
    query = result.writeStream.option('numRows', 300).format("console").trigger(once=True).start().awaitTermination() 
    endTime = time.time()
    print(endTime - startTime) 
 
    