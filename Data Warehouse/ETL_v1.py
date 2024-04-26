# import library 
from pyspark.sql import SparkSession 
import time, datetime
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
        self._autoCreateTable()
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
        
        # filters table is a subtable from symbol infor table 
        # filters_querry=warehouse_model.Filters()
        # self._createTable(tbName="Filters", tbQuerry=filters_querry)
        
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
    
    def _getLastestId(self, old_table, symbol_id=1, column_name=None):
        '''Get the latest id of column from table'''
        latestDF_value=self.spark.sql(f"SELECT {column_name}  FROM {old_table} WHERE symbol_id={symbol_id} ORDER BY {column_name} DESC LIMIT 1;")
        # latestDF_value.show()
        # print(latestDF_value.collect())
        if latestDF_value.isEmpty():
            return 0
        else: 
            latest_value=latestDF_value.collect()[0][f'{column_name}']
            # print(latest_value)
            return latest_value
        
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
            self.etl_Symbol_Infor(raw_symbolInfor)
        
        # extract-transform-load trades table 
        raw_Trades=self.getDataFromDataLake(table="Trades", symbol=symbol)
        if raw_Trades:
            self.etl_trades(raw_Trades, symbol=symbol)
        
        # extract-transform-load Klines table 
        raw_Klines=self.getDataFromDataLake(table='Klines', symbol=symbol)
        if raw_Klines:
            self.etl_Klines(raw_Klines, symbol=symbol)
        
        # extract-transform-load Ticker_24h table 
        raw_Ticker_24h=self.getDataFromDataLake(table='Ticker_24h', symbol=symbol)
        if raw_Ticker_24h:
            self.etl_Ticker_24h(raw_Ticker_24h, symbol=symbol)
    
    
    def etl_Symbol_Infor(self, raw_data):
        n_rows=raw_data.count()
        # print(n_row)
        raw_data_collect=raw_data.collect()
        for i in range(n_rows):
            df=raw_data_collect[i]     
            ''' Extract - transform - load symbol infor table into data warehouse'''
            allowTrailingStop=df['allowTrailingStop']
            baseAsset=df['baseAsset']
            baseAssetPrecision=df['baseAssetPrecision']
            baseCommissionPrecision=df['baseCommissionPrecision']
            cancelReplaceAllowed=df['cancelReplaceAllowed']
            defaultSelfTradePreventionMode=df['defaultSelfTradePreventionMode']
            filters=df['filters']
            icebergAllowed=df['icebergAllowed']
            isMarginTradingAllowed=df['isMarginTradingAllowed']
            isSpotTradingAllowed=df['isSpotTradingAllowed']
            ocoAllowed=df['ocoAllowed']
            quoteAsset=df['quoteAsset'] 
            quoteAssetPrecision=df['quoteAssetPrecision']
            quoteCommissionPrecision=df['quoteCommissionPrecision']
            quoteOrderQtyMarketAllowed=df['quoteOrderQtyMarketAllowed']
            quotePrecision=df['quotePrecision']
            status=df['status']  
            symbol_name=df['symbol']
            
            # get symbol_id from symbol_name 
            symbol_id=self._getSymbol_id(symbol_name)
            
            # get the latest filterstype_id 
            latest_filter_type_id=self._getLastestId(old_table="Symbol_Infor", symbol_id=symbol_id, column_name='filterstype_id') 
       
            insert_Symbol_Infor_qry=f"INSERT INTO Symbol_Infor partition(symbol_id={symbol_id})\
            VALUES('{symbol_name}', \
                '{status}', \
                '{baseAsset}',\
                {baseAssetPrecision},\
                '{quoteAsset}',\
                {quotePrecision},\
                {quoteAssetPrecision},\
                {baseCommissionPrecision},\
                {quoteCommissionPrecision},\
                {latest_filter_type_id + 1},\
                {icebergAllowed},\
                {ocoAllowed},\
                {quoteOrderQtyMarketAllowed},\
                {allowTrailingStop},\
                {cancelReplaceAllowed},\
                {isSpotTradingAllowed},\
                {isMarginTradingAllowed},\
                '{defaultSelfTradePreventionMode}');"
            # print(insert_Symbol_Infor_qry)
            self.spark.sql(insert_Symbol_Infor_qry)
            print(f"Insert {symbol_name} into Symbol_Infor table successfully!")
            
            for filter in filters:
                if(filter['filterType'] == 'PRICE_FILTER'):
                    insert_Price_filter_detail_qry=f"INSERT INTO Price_filter_detail \
                        partition(symbol_id={symbol_id}, filter_type_id={latest_filter_type_id + 1}) \
                        VALUES({float(filter['maxPrice'])},{float(filter['minPrice'])}, {float(filter['tickSize'])});"
                    self.spark.sql(insert_Price_filter_detail_qry)
                      
                if(filter['filterType'] == 'LOT_SIZE'):
                    insert_Lot_size_filter_detail_qry=f"INSERT INTO  Lot_size_filter_detail \
                        partition(symbol_id={symbol_id}, filter_type_id={latest_filter_type_id + 1}) \
                        VALUES({float(filter['stepSize'])}, {float(filter['maxQty'])}, {float(filter['minQty'])});"
                    self.spark.sql(insert_Lot_size_filter_detail_qry)
                
                if(filter['filterType'] == 'ICEBERG_PARTS'):
                    insert_Iceberg_parts_filter_detail_qry=f"INSERT INTO Iceberg_parts_filter_detail \
                        partition(symbol_id={symbol_id}, filter_type_id={latest_filter_type_id + 1}) \
                        VALUES({float(filter['limit'])});"
                    self.spark.sql(insert_Iceberg_parts_filter_detail_qry)
                  
                if(filter['filterType'] == 'TRAILING_DELTA'):
                    insert_Trailing_delta_filter_detail_qry=f"INSERT INTO Trailing_delta_filter_detail\
                        partition(symbol_id={symbol_id}, filter_type_id={latest_filter_type_id + 1}) \
                        VALUES({float(filter['minTrailingBelowDelta'])}, {float(filter['maxTrailingBelowDelta'])}, {float(filter['maxTrailingAboveDelta'])}, \
                            {float(filter['minTrailingAboveDelta'])});"
                    self.spark.sql(insert_Trailing_delta_filter_detail_qry)
                         
                if(filter['filterType'] == 'PERCENT_PRICE_BY_SIDE'):
                    insert_Percent_price_by_side_filter_detail_qry=f"INSERT INTO Percent_price_by_side_filter_detail\
                        partition(symbol_id={symbol_id}, filter_type_id={latest_filter_type_id + 1}) \
                        VALUES({float(filter['bidMultiplierUp'])}, {float(filter['askMultiplierUp'])}, {float(filter['bidMultiplierDown'])}, {float(filter['avgPriceMins'])}, {float(filter['askMultiplierDown'])});"
                    self.spark.sql(insert_Percent_price_by_side_filter_detail_qry)
                
                if(filter['filterType'] == 'NOTIONAL'):
                    insert_Notional_filter_detail_qry=f"INSERT INTO Notional_filter_detail\
                        partition(symbol_id={symbol_id}, filter_type_id={latest_filter_type_id + 1}) \
                        VALUES({float(filter['maxNotional'])}, {float(filter['minNotional'])}, {float(filter['avgPriceMins'])}, {filter['applyMinToMarket']}, {filter['applyMaxToMarket']});"     
                    self.spark.sql(insert_Notional_filter_detail_qry)
                
                if(filter['filterType'] == 'MAX_NUM_ORDERS'):
                    insert_Max_num_orders_filter_detail_qry=f"INSERT INTO Max_num_orders_filter_detail\
                        partition(symbol_id={symbol_id}, filter_type_id={latest_filter_type_id + 1}) \
                        VALUES({float(filter['maxNumOrders'])});"
                    self.spark.sql(insert_Max_num_orders_filter_detail_qry)
                
                if(filter['filterType'] == 'MAX_NUM_ALGO_ORDERS'):
                    insert_Max_num_algo_orders_filter_detail_qry=f"INSERT INTO Max_num_algo_orders_filter_detail\
                        partition(symbol_id={symbol_id}, filter_type_id={latest_filter_type_id + 1}) \
                        VALUES({float(filter['maxNumAlgoOrders'])});"
                    self.spark.sql(insert_Max_num_algo_orders_filter_detail_qry)
    
    def changeTimeIntoTimeStamp(self, milisecondTime):
        # change UTC time to timestamp
        timestamp = datetime.datetime.fromtimestamp(milisecondTime / 1000)
        formatted_timestamp = timestamp.strftime('%Y-%m-%d %H:%M:%S')
        return formatted_timestamp
    
    
    def etl_trades(self, raw_data, symbol):
        n_rows=raw_data.count()
        raw_data_collect=raw_data.collect()
        symbol_id =self._getSymbol_id(symbol)
        for i in range(n_rows):
            row=raw_data_collect[i]
            insert_Trades_qry=f"INSERT INTO Trades\
                        partition(symbol_id={symbol_id})\
                        Values({row['id']}, {row['isBuyerMaker']}, {row['isBestMatch']}, {row['price']},\
                        {float(row['qty'])},  {float(row['quoteQty'])}, '{self.changeTimeIntoTimeStamp(row['time'])}');"
            self.spark.sql(insert_Trades_qry)
        
        print(f"Insert {symbol} into Trades table successfully!")
            
    
    def etl_Klines(self, raw_data, symbol):
        n_rows=raw_data.count()
        raw_data_collect=raw_data.collect()
        symbol_id=self._getSymbol_id(symbol)
        # get the latest klines_id 
        latest_kline_id=self._getLastestId(old_table="Klines", symbol_id=symbol_id, column_name='kline_id') 
        for i in range(n_rows):
            latest_kline_id+=1
            row=raw_data_collect[i]
            insert_Klines_qry=f"INSERT INTO Klines\
                        partition(symbol_id={symbol_id})\
                        VALUES({latest_kline_id},'{self.changeTimeIntoTimeStamp(row['_1'])}', {float(row['_2'])},{float(row['_3'])},\
                        {float(row['_4'])},{float(row['_5'])},{float(row['_6'])},'{self.changeTimeIntoTimeStamp(row['_7'])}',\
                        {float(row['_8'])},{int(row['_9'])},{float(row['_10'])},{float(row['_11'])},{bool(row['_12'])});"
            self.spark.sql(insert_Klines_qry)  
            
        print(f"Insert {symbol} into Klines table successfully!")  
            
    def etl_Ticker_24h(self, raw_data, symbol):
        n_rows=raw_data.count()
        raw_data_collect=raw_data.collect()
        symbol_id=self._getSymbol_id(symbol)
        # get the latest ticker_id 
        latest_ticker_id=self._getLastestId(old_table="Ticker_24h", symbol_id=symbol_id, column_name='ticker_id') 
        for i in range(n_rows):
            latest_ticker_id+=1
            row=raw_data_collect[i]
            print(row)
            insert_Ticker_24h_qry=f"INSERT INTO Ticker_24h\
                        partition(symbol_id={symbol_id})\
                        VALUES({latest_ticker_id}, {float(row['priceChange'])}, {float(row['priceChangePercent'])},\
                        {float(row['weightedAvgPrice'])}, {float(row['prevClosePrice'])}, {float(row['lastPrice'])}, {float(row['bidPrice'])}, \
                        {float(row['askPrice'])}, {float(row['openPrice'])}, {float(row['highPrice'])}, {float(row['lowPrice'])}, \
                        {float(row['volume'])}, '{self.changeTimeIntoTimeStamp(row['openTime'])}', '{self.changeTimeIntoTimeStamp(row['closeTime'])}', {float(row['firstId'])}, \
                        {float(row['lastId'])}, {float(row['count'])});"
            self.spark.sql(insert_Ticker_24h_qry)
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
