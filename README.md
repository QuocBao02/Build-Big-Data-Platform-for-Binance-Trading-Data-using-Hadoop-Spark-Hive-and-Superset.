# **Overview** 
![image](./image/overview.png)

Preparation: Install Hadoop, Spark and Hive on Ubuntu operating system.

**Step 1**: Collect data from the binance.com website using the API, then save all that data to Data Lake as raw data. This process will be performed using Python and Apache Spark (PySpark).

**Step 2:** ETL (Extract, Transform, Load): Use Apache Spark to extract data from Data Lake, perform necessary processing such as filtering, transforming and cleaning data, then save data to Data Lake. Warehouse via Hive. Data will be saved as a relational database (RDB) or reports that can be queried and processed using SQL through Hive.

**Step 3:** Visualize data by creating reports and charts based on data saved on Data Warehouse. Apache Superset can be used to create compelling charts and reports to clearly see important information from the data.

**Step 4:** Use Apache Airflow to automate the entire process. Airflow will help determine the execution schedule of previous steps automatically, ensuring that data collection, ETL, and visualization are performed according to a predetermined schedule

# **Data Source** 
## Binance API [link](https://www.binance.com/en/support/faq/how-to-create-api-keys-on-binance-360002502072)

Binance test api:

`api_key = "aRkqlapnqhNXa1bYU4Q7QWkru6DHA5sdRrmKxnRTPXjbXbZhqOPCJ8p0oNCNNbhY"`

`api_secret = "uK3edZV3Wy2blZHEC67UlsQVgm48JRz1WlWi5ZNrJDg4Aajt3B0QwDMQjOS6cHnH"`
# **Data Lake** 
## Symbol_Infor table 
![image](./image/symbol_infor.png)
## Ticker_24 table 
![image](./image/ticker_24h.png)
## Klines table 
![image](./image/klines.png)
## Trades table 
![image](./image/trade.png)

## Hadoop hdfs
Hadoop hdfs is the location used to store raw data for data lake with partitioned format.

![image](./image/datalake.png)
![image](./image/explicity_datalake.png)
![image](./image/explicity_datalake1.png)

# **Data Warehouse** 
[**Data Warehouse Model**](https://dbdiagram.io/d/64b2209402bd1c4a5e1d07ad)

![image](./image/datamodel.png)

Data Warehouse in Hadoop hdfs

![image](./image/datawarehouse.png)
![image](./image/explicity_datawarehouse.png)
![image](./image/explicity_datawarehouse1.png)

# **Airflow Pipeline** 
![image](./image/airflow.png)
![image](./image/airflow_dags.png)

# **Superset Visualization** 
![image](./image/superset.jpeg)



