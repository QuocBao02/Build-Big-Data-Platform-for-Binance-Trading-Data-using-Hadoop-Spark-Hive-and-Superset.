# import library 
from pyspark.sql import SparkSession 
import time
import pyspark.sql.functions as F
import sys 
import warehouse_model

def getPartitionTime():
    '''Create directory to save data into data lake in hadoop hdfs'''
    current_time=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    year=current_time.split()[0].split('-')[0]
    month=current_time.split()[0].split('-')[1]
    day=current_time.split()[0].split('-')[2]
    partition=f'year={year}/month={month}/day={day}'
    return partition

class ETL(object):
    def __init__(self, database, host, hdfs_port, metastore_port):
        self.database=database
        self.host=host
        self.hdfs_port=hdfs_port
        self.metastore_port=metastore_port
        self.spark=self._initSpark()
        self._createDatabase()
        # self._autoCreateTable()
        self.symbols_dict=self._getSymbolDictFromLogFile()
        self.firstTrade_id=0
        self.lassTrade_id=0
        
        
     
    def _initSpark(self):
        ''' Initialize spark session for ETL job '''
        
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
    
    def stopSpark(self):
        print("Stop Spark Session!")
        self.spark.stop()
        
    def getDataFromDataLake(self, table, symbol):
        ''' Using spark to read from hdfs
        param: Name of table
        return: spark dataframe
        '''
        partition=getPartitionTime()
        hdfs_path=f"hdfs://{self.host}:{self.hdfs_port}/{self.database}/DataLake/{table}/symbol={symbol}/{partition}/{table}.parquet"
        try:
            raw_data=self.spark.read.parquet(hdfs_path)
        except:
            print(f"{symbol} is empty dataframe!")
            return 0
        
        return raw_data
    
    def _createDatabase(self):
        ''' Check dbName in hive datawarehouse and create if not exist
        param: data base's name
        return: dbName will be created in hive datawerehouse
        '''
        querry =f"CREATE DATABASE IF NOT EXISTS {self.database};"
        self.spark.sql(querry)
        print(f"{self.database} was created!")
        self.spark.sql(f"USE {self.database};")
    
    def _createTable(self, tbName, tbQuerry):
        ''' Check tbName in tbQuerry whether created in dbName or not and create if not exist
        params: dbName: data base's Name, tbName: table's Name
        NOTE: dbName had to be created in hive!
        '''
        self.spark.sql(tbQuerry)
        print(f"Table {tbName} was created!")

    def _autoCreateTable(self):
        '''Check and auto create table if not exist when run etl task'''
        # symbol infor table 
        symbol_infor_querry=warehouse_model.Symbol_Infor()
        print(symbol_infor_querry)
        self._createTable(tbName="Symbol_Infor", tbQuerry=symbol_infor_querry)

        # trades table 
        trades_querry=warehouse_model.Trades()
        self._createTable(tbName="Trades", tbQuerry=trades_querry)
        
        # price filter detail table 
        self._createTable(tbName='Price_filter_detail', tbQuerry=warehouse_model.Price_filter_detail())
        
        # Lot_size_filter_detail table 
        self._createTable(tbName='Lot_size_filter_detail', tbQuerry=warehouse_model.Lot_size_filter_detail())
        
        # Iceberg_parts_filter_detail table
        self._createTable(tbName='Iceberg_parts_filter', tbQuerry=warehouse_model.Iceberg_parts_filter_detail())
        
        # Market_lot_size_filter_detail table 
        self._createTable(tbName="Market_lot_size_filter_detail", tbQuerry=warehouse_model.Market_lot_size_filter_detail())
        
        # Trailing_delta_filter_detail table 
        self._createTable(tbName="Trailing_delta_filter_detail", tbQuerry=warehouse_model.Trailing_delta_filter_detail())

        # Percent_price_by_side_filter_detail table         
        self._createTable(tbName="Percent_price_by_side_filter_detail", tbQuerry=warehouse_model.Percent_price_by_side_filter_detail())
        
        # Notional_filter_detail table 
        self._createTable(tbName="Notional_filter_detail", tbQuerry=warehouse_model.Notional_filter_detail())
        
        # Max_num_orders_filter_detail table 
        self._createTable(tbName="Max_num_orders_filter_detail", tbQuerry=warehouse_model.Max_num_orders_filter_detail())
        
        # Max_num_algo_orders_filter_detail table 
        self._createTable(tbName="Max_num_algo_orders_filter_detail", tbQuerry=warehouse_model.Max_num_algo_orders_filter_detail())
        
        # Ticker_24h table 
        self._createTable(tbName="Ticker_24h", tbQuerry=warehouse_model.Ticker_24h())
        
        # Klines table 
        self._createTable(tbName="Klines", tbQuerry=warehouse_model.Klines())
        
        pass
    
    def _getSymbolDictFromLogFile(self, argv=sys.argv):
        if len(argv) < 2:
            print('Argument values from input is less expected!')
            return 0 
        else:
            log_path_file=argv[1]
            with open(log_path_file, 'r') as f:
                string_symbols_dict=f.readlines()
            symbols_dict=eval(string_symbols_dict[-2])
            return symbols_dict
    
    def _getLastestId(self, old_table, symbol, column_name=None):
        '''Get the latest id of column from table'''
        try:
            latestDF_value=self.spark.sql(f"SELECT {column_name}  FROM {old_table} WHERE symbol='{symbol}' ORDER BY {column_name} DESC LIMIT 1;")
            # latestDF_value.show()
            # print(latestDF_value.collect())
            if latestDF_value.isEmpty():
                return 0
            else: 
                latest_value=latestDF_value.collect()[0][f'{column_name}']
                # print(latest_value)
                return latest_value
        except:
            return 0
            
    def _getSymbol_id(self, symbol):
        symbol_id=0 
        for id in self.symbols_dict.keys():
            if self.symbols_dict[id] == symbol:
                symbol_id=id
                break 
        return symbol_id
    
    # elt
    def elt(self, symbol):
    # Table symbol_infor 
        # extract -transform-load Symbol Infor table 
        raw_symbolInfor=self.getDataFromDataLake(table='SymbolInfor',symbol=symbol) # get raw data from datalake 
        if raw_symbolInfor:
            self.etl_Symbol_Infor(raw_symbolInfor, symbol)
        
        # # extract-transform-load trades table 
        raw_Trades=self.getDataFromDataLake(table="Trades", symbol=symbol)
        if raw_Trades:
            self.etl_trades(raw_Trades, symbol=symbol)
        
        # # extract-transform-load Klines table 
        raw_Klines=self.getDataFromDataLake(table='Klines', symbol=symbol)
        if raw_Klines:
            self.etl_Klines(raw_Klines, symbol=symbol)
        
        # # extract-transform-load Ticker_24h table 
        raw_Ticker_24h=self.getDataFromDataLake(table='Ticker_24h', symbol=symbol)
        if raw_Ticker_24h:
            self.etl_Ticker_24h(raw_Ticker_24h, symbol=symbol)
    
    
    def etl_Symbol_Infor(self, raw_data, symbol):
        
        # create a temporary table 
        raw_data.createOrReplaceTempView("symbol_infor_temp_tb")
        
        # get symbol_id from symbol_name 
        # symbol_id=self._getSymbol_id(symbol)
        
        # get the latest filterstype_id 
        latest_filter_type_id=self._getLastestId(old_table="Symbol_Infor", symbol=symbol, column_name='filterstype_id')
        
        transform_load_symbol=self.spark.sql(f"\
        SELECT allowTrailingStop ,\
            baseAsset ,\
            baseAssetPrecision ,\
            baseCommissionPrecision ,\
            cancelReplaceAllowed ,\
            defaultSelfTradePreventionMode ,\
            {latest_filter_type_id+1} AS filterstype_id  ,\
            icebergAllowed ,\
            isMarginTradingAllowed ,\
            isSpotTradingAllowed ,\
            ocoAllowed ,\
            quoteAsset ,\
            quoteAssetPrecision ,\
            quoteCommissionPrecision ,\
            quoteOrderQtyMarketAllowed ,\
            quotePrecision ,\
            status ,\
            symbol\
        FROM symbol_infor_temp_tb;")
        
        transform_load_symbol.write.partitionBy('symbol').mode("append").format("parquet").saveAsTable(f"{self.database}.symbol_infor")
        print(f"Insert {symbol} into Symbol_Infor table successfully!")
            
        filters=self.spark.sql("""SELECT filters from symbol_infor_temp_tb;""")
        filters=filters.collect()[0]['filters']
        
        for filter in filters:
        #     print(filter)
            if(filter['filterType'] == 'PRICE_FILTER'):
                filter['symbol']=symbol
                filter['filter_type_id']=latest_filter_type_id + 1
                df=self.spark.createDataFrame([filter])
                df.write.partitionBy(['symbol', 'filter_type_id']).mode("append").format("parquet").saveAsTable(f"{self.database}.price_filter_detail")
                print(f"Insert {symbol} into price_filter_detail table successfully!")
                    
            if(filter['filterType'] == 'LOT_SIZE'):
                filter['symbol']=symbol
                filter['filter_type_id']=latest_filter_type_id + 1
                df=self.spark.createDataFrame([filter])
                df.write.partitionBy(['symbol', 'filter_type_id']).mode("append").format("parquet").saveAsTable(f"{self.database}.lot_size_detail")
                print(f"Insert {symbol} into lot_size_detail table successfully!")
            
            if(filter['filterType'] == 'ICEBERG_PARTS'):
                filter['symbol']=symbol
                filter['filter_type_id']=latest_filter_type_id + 1
                df=self.spark.createDataFrame([filter])
                df.write.partitionBy(['symbol', 'filter_type_id']).mode("append").format("parquet").saveAsTable(f"{self.database}.iceberg_parts_detail")
                print(f"Insert {symbol} into iceberg_parts_detail table successfully!")
                
            if(filter['filterType'] == 'TRAILING_DELTA'):
                filter['symbol']=symbol
                filter['filter_type_id']=latest_filter_type_id + 1
                df=self.spark.createDataFrame([filter])
                df.write.partitionBy(['symbol', 'filter_type_id']).mode("append").format("parquet").saveAsTable(f"{self.database}.trailing_delta")
                print(f"Insert {symbol} into trailing_delta table successfully!")
                        
            if(filter['filterType'] == 'PERCENT_PRICE_BY_SIDE'):
                filter['symbol']=symbol
                filter['filter_type_id']=latest_filter_type_id + 1
                df=self.spark.createDataFrame([filter])
                df.write.partitionBy(['symbol', 'filter_type_id']).mode("append").format("parquet").saveAsTable(f"{self.database}.percent_price_by_side_detail")
                print(f"Insert {symbol} into percent_price_by_side_detail table successfully!")
            
            if(filter['filterType'] == 'NOTIONAL'):
                filter['symbol']=symbol
                filter['filter_type_id']=latest_filter_type_id + 1
                df=self.spark.createDataFrame([filter])
                df.write.partitionBy(['symbol', 'filter_type_id']).mode("append").format("parquet").saveAsTable(f"{self.database}.notional_detail")
                print(f"Insert {symbol} into notional_detail table successfully!")
            
            if(filter['filterType'] == 'MAX_NUM_ORDERS'):
                filter['symbol']=symbol
                filter['filter_type_id']=latest_filter_type_id + 1
                df=self.spark.createDataFrame([filter])
                df.write.partitionBy(['symbol', 'filter_type_id']).mode("append").format("parquet").saveAsTable(f"{self.database}.max_num_orders_detail")
                print(f"Insert {symbol} into max_num_orders_detail table successfully!")
            
            if(filter['filterType'] == 'MAX_NUM_ALGO_ORDERS'):
                filter['symbol']=symbol
                filter['filter_type_id']=latest_filter_type_id + 1
                df=self.spark.createDataFrame([filter])
                df.write.partitionBy(['symbol', 'filter_type_id']).mode("append").format("parquet").saveAsTable(f"{self.database}.max_num_algo_orders_detail")
                print(f"Insert {symbol} into max_num_algo_orders_detail table successfully!")
    
    
    def etl_trades(self, raw_data, symbol):
        # create a temporary table 
        raw_data.createOrReplaceTempView("trades_temp_tb")
        
        # get symbol_id from symbol_name 
        # symbol_id=self._getSymbol_id(symbol)
        
        transform_trade_table=self.spark.sql(f"\
            SELECT '{symbol}' AS symbol, \
                id AS trade_id,\
                isBuyerMaker,\
                isBestMatch,\
                price,\
                qty,\
                quoteQty,\
                FROM_UNIXTIME(time/1000) as trade_time\
                FROM trades_temp_tb;")
        # transform_trade_table.show(1000)
        transform_trade_table.write.partitionBy('symbol').mode('append').format('parquet').saveAsTable(f"{self.database}.Trades")
        print(f"Insert {symbol} into Trades table successfully!")
    
    def etl_Klines(self, raw_data, symbol):
        # create a temporary table 
        raw_data.createOrReplaceTempView("klines_temp_tb")
        
        # get the latest klines_id 
        latest_kline_id=self._getLastestId(old_table="Klines", symbol=symbol, column_name='kline_id') 
        
        
        # transform_klines_table.show()
        transform_klines_table=self.spark.sql(f"\
            SELECT '{symbol}' AS symbol, \
                FROM_UNIXTIME(_1/1000) AS opentime, \
                _2    AS openPrice, \
                _3    AS highPrice, \
                _4    AS lowPrice, \
                _5    AS closePrice, \
                _6    AS volume, \
                FROM_UNIXTIME(_7/1000) AS closetime, \
                _8    AS quoteassetvolume, \
                _9    AS numberoftra, \
                _10    AS takerbaseassetvolume, \
                _11    AS takerbuyquoteassetvolume, \
                _12    AS ignored\
            FROM klines_temp_tb;")
        # Add a new column with monotonically increasing IDs
        transform_klines_table = transform_klines_table.withColumn("kline_id", F.monotonically_increasing_id() + latest_kline_id + 1)
        transform_klines_table.write.partitionBy('symbol').mode('append').format('parquet').saveAsTable(f"{self.database}.Klines")
        
        print(f"Insert {symbol} into Klines table successfully!")  
            
    def etl_Ticker_24h(self, raw_data, symbol):
        # create a temporary table 
        raw_data.createOrReplaceTempView("ticker_24h_tb")
        
        # get symbol_id from symbol_name 
        # symbol_id=self._getSymbol_id(symbol)
        # get the latest ticker_id 
        latest_ticker_id=self._getLastestId(old_table="Ticker_24h", symbol=symbol, column_name='ticker_id') 

        transform_ticker_24h_table=self.spark.sql(f"\
            SELECT '{symbol}' as symbol, \
                {latest_ticker_id + 1} as ticker_id, \
                priceChange ,\
                priceChangePercent ,\
                weightedAvgPrice ,\
                prevClosePrice ,\
                lastPrice ,\
                bidPrice ,\
                askPrice ,\
                openPrice ,\
                highPrice ,\
                lowPrice ,\
                volume ,\
                FROM_UNIXTIME(openTime/1000) AS openTime ,\
                FROM_UNIXTIME(closeTime/1000) AS closeTime ,\
                firstId AS fristTradeId, \
                lastId AS lastTradeId, \
                count\
            FROM ticker_24h_tb;")
        # transform_ticker_24h_table.show()
        transform_ticker_24h_table.write.partitionBy('symbol').mode('append').format('parquet').saveAsTable(f"{self.database}.Ticker_24h")
        print(f"Insert {symbol} into Ticker_24h table successfully!")
    
def main():  
    # configuration 
    hdfs_host='localhost'
    hdfs_port=9000
    metastore_port=9083
    database='Binance_Market_Data'

    etl=ETL(database, hdfs_host, hdfs_port, metastore_port)
    symbols_dict=etl.symbols_dict 
    for key in symbols_dict.keys():
        symbol=symbols_dict[key]
        etl.elt(symbol)
    
    etl.stopSpark()
    
    
if __name__=='__main__':
    main()
