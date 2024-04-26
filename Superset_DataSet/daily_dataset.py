from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime, timedelta

class SuperSet(object):
    def __init__(self, current_time, prev_time):
        self.spark = self._initSpark()
        self._create_Change_daily_db()
        self.current_date = current_time
        self.prev_date = prev_time
        
        
    def _initSpark(self):
        # create spark session connect to hive data warehouse 
        spark = SparkSession.builder\
            .appName("InitSparkForVisualization")\
            .config("hive.metastore.uris", "thrift://localhost:9083")\
            .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/Binance_Market_Data/datawarehouse/")\
            .enableHiveSupport()\
            .getOrCreate()
        if spark:
            print("Spark Session is CREATED!")
            return spark
        else:
            print("Spark Session can not create!")


    def _create_Change_daily_db(self):
        # create daily dataset named: changes_daily.db 
        self.spark.sql("""create database if not exists changes_daily;""")


    # join table 
    
    # number_of_trades_symbols table
    def number_of_trades_symbols_tb(self):
        while True:
            try: 
                trades_df = self.spark.sql(f"select symbol, count(*) as NumberOfTrades\
                    from binance_market_data.trades\
                    where trade_time between '{self.current_date}' and '{self.prev_date}'\
                    group by symbol;")
                break
            except:
                print("Can not get number_of_trades_symbols")
                print("Reloading ...")
                
        trades_df.show(20)
        trades_df.write.mode("overwrite").saveAsTable("changes_daily.number_of_trades_symbols")
        
    def total_trades(self):
        
        # read old trades in data warehouse 
        old_trades_num = 0
        try: 
            old_trades = self.spark.sql("select TotalTrades from changes_daily.total_trades")
            if old_trades.isEmpty() == False and old_trades.collect()[0]['TotalTrades'] is not None:
                old_trades_num = old_trades.collect()[0]['TotalTrades']
        except:
            old_trades_num = 0
        # read the total daily trades 
        while True:
            try: 
                total_trades = self.spark.sql(f"select sum(NumberOfTrades) + {old_trades_num} as TotalTrades from changes_daily.number_of_trades_symbols")
                break
            except:
                print("Can not get trades data")
                print("Reloading ...") 
                
        total_trades.show(20)
        total_trades.write.mode("overwrite").saveAsTable("changes_daily.total_trades")
    

    # BTC line chart 
    def BTCUSDT_trades(self):
        # read daily trades 
        while True:
            try: 
                BTC_trades = self.spark.sql(f"select trade_id, price, qty, trade_time\
                    from binance_market_data.trades\
                    where symbol='BTCUSDT' and trade_time between '{self.current_date}' and '{self.prev_date}'\
                    order by trade_time;")
                break
            except:
                print("Can not get BTC trades data")
                print("Reloading ...") 
                
        BTC_trades.show(10)
        BTC_trades.write.mode("overwrite").saveAsTable("changes_daily.BTCUSDT_trades")
        
    def BTCUSDT_klines(self):
        # read daily kline 
        while True:
            try: 
                BTC_klines = self.spark.sql(f"\
                    select opentime, openprice, closeprice, highprice, lowprice\
                    from binance_market_data.klines\
                    where symbol='BTCUSDT' and opentime between '{self.current_date}' and '{self.prev_date}'\
                    order by opentime;")
                break
            except:
                print("Can not get BTC trades data")
                print("Reloading ...") 
        # BTC_klines.show(10)
        BTC_klines.write.mode("overwrite").saveAsTable("changes_daily.BTCUSDT_klines")
                
def get_current_time():
        current = datetime.now()
        current_date= current.date()
        current_time=current.time()
        
        one_day= timedelta(days=1)
        time = timedelta(hours=current_time.hour, minutes=current_time.minute, seconds=current_time.second)
        
        if current_date.day == 1:
            prev_date = datetime(current_date.year, current_date.month, 1) - one_day 
        else:
            prev_date = current_date - one_day
        
        current_date_tstp = datetime.combine(current_date, datetime.min.time()) + time
        prev_date_tstp = datetime.combine(prev_date, datetime.min.time()) + time 
     
        return (current_date_tstp.strftime("%Y-%m-%d %H:%M:%S"), prev_date_tstp.strftime("%Y-%m-%d %H:%M:%S"))



def main():
    current_time, prev_time = get_current_time()
    # print(current_time)
    # print(prev_time) 
    superset=SuperSet(current_time, prev_time)
    superset.number_of_trades_symbols_tb()
    superset.total_trades()
    superset.BTCUSDT_trades()
    superset.BTCUSDT_klines()

    superset.spark.stop()
    
if __name__=="__main__":
    main()
    
