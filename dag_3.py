from copy import copy
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.operators.dummy_operator import DummyOperator
from airflow import DAG
from datetime import datetime
from airflow.operators.python_operator import PythonOperator

def load_data_into_raw_table():
        hook = SnowflakeHook(snowflake_conn_id="snowflake_df")
        sql = """
        PUT file:///opt/airflow/dags/data3.csv @my_stage; 
        COPY INTO RAW_TABLE FROM @my_stage FILE_FORMAT = (FORMAT_NAME = 'my_csv_format', ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE) ON_ERROR = 'CONTINUE';
        """
        hook.run(sql)

def load_data_from_raw_to_stage():
        hook = SnowflakeHook(snowflake_conn_id="snowflake_df")
        sql = """
        INSERT INTO AIRFLOW_SCHEMA.STAGE_TABLE (DATE, OPEN, HIGH, LOW, CLOSE, VOLUME, TICKER) SELECT DATE, OPEN, HIGH, LOW, CLOSE, VOLUME, TICKER FROM AIRFLOW_SCHEMA.RAW_STREAM WHERE METADATA$ACTION = 'INSERT'
        """
        hook.run(sql)

def load_data_from_stage_to_master():
        hook = SnowflakeHook(snowflake_conn_id="snowflake_df")
        sql = """
        INSERT INTO AIRFLOW_SCHEMA.MASTER_TABLE (DATE, OPEN, HIGH, LOW, CLOSE, VOLUME, TICKER) SELECT DATE, OPEN, HIGH, LOW, CLOSE, VOLUME, TICKER FROM AIRFLOW_SCHEMA.STAGE_STREAM WHERE METADATA$ACTION = 'INSERT';
        """
        hook.run(sql)



with DAG("dag_3", start_date=datetime.now(), schedule_interval=None, catchup=False) as dag:

    start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

    
    load_raw = PythonOperator(
            task_id='load_data_into_raw_table',
            python_callable=load_data_into_raw_table,
            dag=dag)

    load_stage = PythonOperator(
            task_id='load_data_from_raw_to_stage',
            python_callable=load_data_from_raw_to_stage,
            dag=dag)

    load_master = PythonOperator(
            task_id='load_data_from_stage_to_master',
            python_callable=load_data_from_stage_to_master,
            dag=dag)
    


start_operator >> load_raw >> load_stage >> load_master

