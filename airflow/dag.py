# import the libraries 
from datetime import timedelta 
from airflow import DAG 
from airflow.operators.bash_operator import BashOperator 
from airflow.utils.dates import days_ago


target_time = 11
hour=target_time - 7 # 7 is the time zone of VietNam 

# define DAG arguments 
default_args={
    'owner': 'Quoc Bao',
    'start_date': days_ago(1),
    'email': ['baonguyen022002499@gmail.com'], 
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# define the DAG 
dag=DAG(
    dag_id="Ingest_Extract_Transform_Load_Binance_Market_Data",
    default_args=default_args,
    description=" Auto Ingest Data from Binance into DataLake, ETL into Datawarehouse",
    schedule_interval=f"15 {hour} * * *",
)

# define the ingestion data from Binance task 
ingestion= BashOperator(
    task_id='ingestion', 
    bash_command='python3 /home/quocbao/MyData/Seminar-Data-Engineering/Data\ Lake/Ingestion.py', 
    dag=dag,
)
# define the etl task 
etl= BashOperator(
    task_id='etl',
    bash_command='python3 /home/quocbao/MyData/Seminar-Data-Engineering/Data\ Warehouse/ETL_v2.py \
        /home/quocbao/MyData/Seminar-Data-Engineering/Data\ Lake/log.txt',
    dag=dag,
)
# define the visualization task 
visualize=BashOperator(
    task_id='visualize',
    bash_command='python3 /home/quocbao/MyData/Seminar-Data-Engineering/Superset_DataSet/daily_dataset.py',
    dag=dag,
)

# task pipeline 
ingestion >> etl >> visualize
