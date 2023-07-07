# Robert Jones
# 6.13.23
# Analytical ETL

import datetime
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import col, avg, when, last

spark = SparkSession.builder.master('local')\
    .config('spark.jars.packages', 'org.apache.hadoop:hadoop-azure:3.3.1')\
    .appName('app')\
    .getOrCreate()
container_name = 'springboardcontainer'
storage_account_name = 'springboardstorage'
output_path = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/combined_trade_and_quote/result"
data_path = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/data/stock_data.parquet" 
azure_key = open('azure_key.txt').read()
spark.conf.set(f"fs.azure.account.key.springboardstorage.blob.core.windows.net",azure_key)

date_str = '2020-08-06'
date = datetime.datetime.strptime(date_str,'%Y-%m-%d')

class AnalyticalETL():

    def __init__(self):
        self.df = spark.read.load(path=data_path,format='parquet',inferSchema='true',header='true') # Load parquet file

    def avg_trade_price(self):

        self.df.createOrReplaceTempView('trades') # Create temp table
        df = spark.sql(f"SELECT * FROM trades WHERE trade_dt = '{date_str}' AND event_type = 'T'") # Select columns by date
        df = df.withColumn('event_tm',df.event_tm.cast('long')) # TimeStamp to Seconds
        # Window function to partition by symbol and gather time for every 30 mins
        w = Window.partitionBy("symbol").orderBy("event_tm").rangeBetween(-1800, 0) # Seconds (-1800) to Minutes (30)
        # Apply window function
        df = df.withColumn('moving_avg', avg(col('price')).over(w))
        df = df.withColumnRenamed('moving_avg','mov_avg_pr')
        # Cast to TimeStamp
        df_trades = df.withColumn('event_tm',df.event_tm.cast('timestamp'))

        return df_trades


    def previous_day_last_trade(self):
        prev_date = (date - datetime.timedelta(days=1)).date()
        self.df.createOrReplaceTempView('tmp_last_trade')
        df = spark.sql(f"SELECT * FROM tmp_last_trade WHERE trade_dt = '{prev_date}' AND event_type = 'T'") # Select columns by date
        df = df.withColumn('event_tm',df.event_tm.cast('long')) # TimeStamp to Seconds
        # Apply window function
        w = Window.partitionBy("symbol").orderBy("event_tm").rangeBetween(-1800, 0) # Seconds (-1800) to Minutes (30)
        df = df.withColumn('moving_avg', avg(col('price')).over(w))
        # Rename moving_avg to mov_avg_pr for consistancy
        df = df.withColumnRenamed('moving_avg','mov_avg_pr')
        # Cast to TimeStamp
        df = df.withColumn('event_tm',df.event_tm.cast('timestamp'))
        # Rank window to find most recent event_tm per symbol
        rank_window = Window.partitionBy('symbol').orderBy(F.desc('event_tm'))
        df = df.withColumn('rank',F.row_number().over(rank_window)).filter(col('rank') == 1)
        # Drop Rank column
        df = df.drop('rank')
        # Rename price to trade_pr for consistancy
        df = df.withColumnRenamed('price','trade_pr')

        return df

    def combine_quotes_and_trades(self):
        self.df.createOrReplaceTempView('quotes') # Create temp table
        # Get Quotes
        df_quotes = spark.sql(f"SELECT * FROM quotes WHERE trade_dt = '{date_str}' AND event_type = 'Q'")
        # Get Trades (w/ rolling average)
        df_trades = AnalyticalETL.avg_trade_price(self)
        # Union dataframes
        df = df_quotes.unionByName(df_trades,allowMissingColumns=True)
        # Sort
        df = df.sort(col('symbol'),col('event_tm').desc())
        # Change column names for clarity
        df = df.withColumnRenamed('moving_avg','mov_avg_pr')\
               .withColumnRenamed('price','trade_pr')
        
        return df
    
    def insert_into_quote(self):
        # Combined quotes and trades DF
        df = AnalyticalETL.combine_quotes_and_trades(self)
        # Last days trades
        df_yesterday = AnalyticalETL.previous_day_last_trade(self)
        # Add last days trades
        df = df.unionByName(df_yesterday,allowMissingColumns=True)
        # Window Function to fill Quote trade_pr and mov_avg_tr with most recent (previous) trade_pr and mov_avg_tr
        windowSpec = Window.partitionBy("symbol").orderBy("event_tm").rowsBetween(Window.unboundedPreceding, Window.currentRow)
        # Apply Window Function
        df = df.withColumn("filled_trade_pr", last(col("trade_pr"), ignorenulls=True).over(windowSpec))
        df = df.withColumn("filled_mov_avg_pr", last(col("mov_avg_pr"), ignorenulls=True).over(windowSpec))
        # Fill columns with window function values
        df = df.withColumn("trade_pr", when(col("trade_pr").isNull(), col("filled_trade_pr")).otherwise(col("trade_pr")))
        df = df.withColumn("mov_avg_pr", when(col("mov_avg_pr").isNull(), col("filled_mov_avg_pr")).otherwise(col("mov_avg_pr")))
        # Drop window function columns (unneeded)
        df = df.drop("filled_trade_pr", "filled_mov_avg_pr")
        # Sort by symbol, then event_tm
        df = df.sort(col('symbol'),col('event_tm').desc())
        # Write to blob
        df.write.mode(saveMode='overwrite').parquet(output_path)

    


instance = AnalyticalETL()
instance.previous_day_last_trade()
instance.insert_into_quote()