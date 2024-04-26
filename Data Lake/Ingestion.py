# import library 
import binance
from  pyspark.sql import SparkSession
import time

# handle request time out
from requests.exceptions import ReadTimeout


class Binance_Ingestion_Data_Lake(object):
    def __init__(self, api_key=None, secret_key=None, database="Binance_Market_Data", hdfs_host="localhost", hdfs_port=9000, thrift_host='localhost', thrift_port=9083):
        self.api_key=api_key 
        self.secret_key=secret_key 
        self.database=database
        self.hdfs_host=hdfs_host
        self.hdfs_port=hdfs_port
        self.host=thrift_host
        self.metastore_port=thrift_port
        self.step=1000
        self.request_per_minute=6000
        self.amount_in_12h=720
        self.currentTime=None
        self.spark=self._connect_DataWarehouse()
        self.binance_cli=self._connect_Binance()
        self.spark=self._initSpark()
        self.partition=self._getPartitionTime()
        self.all_symbols=self._get_all_symbols()
        
    def _connect_Binance(self):
        try: 
            print("Requesting with Binance Market Data")
            binance_client=binance.Client(api_key=self.api_key, api_secret=self.secret_key,requests_params={"timeout": 10})
        except: 
            print("Can not connect to Binance!")
        else:
            print("Connected to Binance Successfully!")
            return binance_client
    def _connect_DataWarehouse(self):
        ''' Initialize spark session for connecting to data warehouse '''
        
        spark=SparkSession.builder\
            .appName("InitSparkSessionForETL")\
            .config("hive.metastore.uris", f"thrift://{self.host}:{self.metastore_port}")\
            .config("spark.sql.warehouse.dir", f"hdfs://{self.host}:{self.hdfs_port}/Binance_Market_Data/datawarehouse/")\
            .enableHiveSupport()\
            .getOrCreate()
        if spark:
            print("Spark Session is CREATED!")
            return spark
        else:
            print("Spark Session can not create!")
    def disconnect_Binance(self):
        '''disconnect Binance'''
        if self.binance_cli: 
            self.binance_cli.close_connection() 
        print('Binance connection is closed!')
        
    def _initSpark(self):
        ''' Initialize Spark Session
        '''
        # create spark session 
        spark=SparkSession.builder.appName("AppendDataToDataLake").getOrCreate()
        return spark
    
    def closeSpark(self):
        '''Close Spark Session'''
        self.spark.stop()
        print("Spark Session is Closed!")
    
    def _getPartitionTime(self):
        '''Create directory to save data into data lake in hadoop hdfs'''
        current_time=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        self.currentTime=current_time
        
        year=current_time.split()[0].split('-')[0]
        month=current_time.split()[0].split('-')[1]
        day=current_time.split()[0].split('-')[2]
        partition=f'year={year}/month={month}/day={day}'
        return partition
    
    def _generatePartition(self, table, symbol):
        '''Generate the path to save into datalake'''
        hdfs_path=f"hdfs://{self.hdfs_host}:{self.hdfs_port}/{self.database}/DataLake/{table}/symbol={symbol}/{self.partition}/{table}.parquet"
        return hdfs_path 
    
    # get all tables data with all symbols
    # it is not used 
    def _get_all_symbols(self):
        ''' The temporary function to get all symbols '''
        all_symbols=self.binance_cli.get_all_tickers()
        df=self.spark.createDataFrame(all_symbols)
        symbols_col=df.select("symbol").distinct()
        symbol_list=[row.symbol for row in symbols_col.collect()]
        size_of_list=len(symbol_list)
        symbol_dict={i:symbol_list[i] for i in range(size_of_list)}
        return symbol_dict
    
    def getSymbolInfor(self, symbol):
        '''Get the information of a symbol and save it into datalake'''
        table='SymbolInfor'
        symbol_info = self.binance_cli.get_symbol_info(symbol=symbol)
        if self._check_emptyDF(symbol_info) == False:
            hdfs_destination=self._generatePartition(table, symbol)
            df=self.spark.createDataFrame([symbol_info])
            df.write.parquet(hdfs_destination, mode='append')           
            print(f"Loaded into {table} successfully!")
        pass
    
    def _getLastestTradeId(self, symbol):
        ''' Get the latest trade id of trades table'''
        try:
            self.spark.sql("USE binance_market_data;")
            
            latest_TradeId=self.spark.sql(f"SELECT trade_id FROM Trades WHERE symbol='{symbol}' ORDER BY trade_id DESC LIMIT 1;")
            print(latest_TradeId)
            if latest_TradeId.isEmpty():
                return 0;
            else:
                latest_TradeId_value=latest_TradeId.collect()[0]['trade_id']
                return latest_TradeId_value
        except:
            return 0
        
    def getTicker_24h(self, symbol):
        '''Get the information of Ticker in 24h of a symbol and save it into datalake'''
        table='Ticker_24h'
        ticker_24h=self.binance_cli.get_ticker(symbol=symbol)
        # get firstId and lastId 
        check_first_id=self._getLastestTradeId(symbol)
        if check_first_id==0:
            self.firstId=ticker_24h['firstId']
        else:
            self.firstId=check_first_id+1
            ticker_24h['firstId'] = self.firstId
            
        self.lastId=ticker_24h['lastId']
        # get openTime and closeTime
        self.openTime=ticker_24h['openTime']
        self.closeTime=ticker_24h['closeTime']
        ticker_24h['count']=self.lastId-self.firstId
        hdfs_destination=self._generatePartition(table, symbol)
        if self._check_emptyDF(ticker_24h) == False:
            df=self.spark.createDataFrame([ticker_24h])
            df.write.parquet(hdfs_destination, mode='append')     
            print(f"Loaded into {table} successfully!")
        pass
    
    
    def getTrades(self, symbol):
        '''Get all trades of a symbol in a day and save it into datalake'''
        table='Trades'
        hdfs_destination=self._generatePartition(table, symbol)
        
        trades_data = []
        # check the validation of fromid and lastid
        if self.firstId > 0 and self.lastId > 0:
            for id in range(self.firstId, self.lastId+1, self.step):
                
                # handle request time out.
                while True:
                    try:
                        trades=self.binance_cli.get_historical_trades(symbol=symbol, fromId=id, limit=1000)
                        trades_data = trades_data + trades
                        break
                    except ReadTimeout:
                        time.sleep(5)
                        
                # sleep for a short time
                time.sleep(60/(self.request_per_minute - 1))
                
            if self._check_emptyDF(trades_data) == False:
                    df=self.spark.createDataFrame(trades_data)
                    df.write.parquet(hdfs_destination, mode='append') 
            print(f"Loaded into {table} successfully!")
        pass
    
    def getKlines(self, symbol):
        '''Get the klines of all trades of a symbol in a day and save it into datalake'''
        table="Klines"
        hdfs_destination=self._generatePartition(table, symbol)
        
        klines_data = []
        for starttime in range(self.openTime, self.closeTime + 1, self.amount_in_12h*60*1000):
            # handle request time out
            while True:
                try:
                    klines=self.binance_cli.get_klines(symbol=symbol, interval=binance.Client.KLINE_INTERVAL_1MINUTE, startTime=starttime, limit=self.amount_in_12h)
                    klines_data = klines_data + klines
                    break 
                except ReadTimeout:
                    time.sleep(5)
    
            # sleep for a short time
            time.sleep(60/(self.request_per_minute - 1))
        if self._check_emptyDF(klines_data) == False:
                df=self.spark.createDataFrame(klines_data)
                df.write.parquet(hdfs_destination, mode='append')
        print(f"Loaded into {table} successfully!")
        pass    
    
    def log_datalake(self, startTime, endTime, symbols):
        start_string=f"Started ingestion data into datalake at {startTime}\n"
        end_string=f"\nFinished ingestion data into datalake at {endTime}\n"
        data=start_string+str(symbols)+end_string
        
        with open('/home/quocbao/MyData/Seminar-Data-Engineering/Data Lake/log.txt', 'a') as f:
            f.write(data)
        
    
    def _check_emptyDF(self, df):
        if len(df) != 0:
            return False 
        else: 
            return True

def get_api(path):
    '''Get api from local file'''
    with open(path, 'r') as f:
        api=f.read().split('\n')
        api_key=api[0]
        secret_key=api[1]
    return(api_key, secret_key)


def getCurrentTime():
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())


def main():   
    # api_path='/home/quocbao/MyData/Binance_API_Key/binance_api_key.txt'
    api_key = "aRkqlapnqhNXa1bYU4Q7QWkru6DHA5sdRrmKxnRTPXjbXbZhqOPCJ8p0oNCNNbhY"
    secret_key = "uK3edZV3Wy2blZHEC67UlsQVgm48JRz1WlWi5ZNrJDg4Aajt3B0QwDMQjOS6cHnH"
    # (api_key, secret_key)=get_api(path=api_path)
    Database="Binance_Market_Data"
    startTime=getCurrentTime()
    binance_datalake=Binance_Ingestion_Data_Lake(api_key=api_key, secret_key=secret_key, database=Database)
    all_symbols=binance_datalake.all_symbols
    symbols={key: all_symbols[key] for key in range(100)}
    
    for symbol in symbols.values():
        print(f"\n {symbol}:")
        binance_datalake.getSymbolInfor(symbol=symbol)
        binance_datalake.getTicker_24h(symbol=symbol)
        binance_datalake.getTrades(symbol=symbol)
        binance_datalake.getKlines(symbol=symbol)
    
    endTime=getCurrentTime()
    binance_datalake.log_datalake(startTime, endTime, symbols)
    
    binance_datalake.closeSpark()
    binance_datalake.disconnect_Binance()
    
if __name__=='__main__':
    main()
    pass