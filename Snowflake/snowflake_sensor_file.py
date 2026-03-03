from airflow.decorators import task, dag, task_group
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator
from airflow.utils.edgemodifier import Label
from airflow.providers.snowflake.transfers.copy_into_snowflake import *
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.email import send_email
from airflow import Dataset
from dotenv import load_dotenv
import os
import pandas as pd

load_dotenv()
path_to_folder = os.getenv('PATH_FOLDER')
file = os.getenv('FILE')
user_email = os.getenv('AIRFLOW__SMTP__SMTP_USER')
final_file = os.getenv('FINAL_FILE')
stage_name = os.getenv('STAGE')
file_format = os.getenv('FORMAT')
file_path = f'{path_to_folder}{file}'
    

def send_silver_success_email(context):
    '''This func send failure notification if task on silver stage success'''
    print(f'CONTEXT :{context}')
    subject = f"Airflow Success: {context['task_instance'].task_id}"
    log_url = context['task_instance'].log_url
    body = f"""
    <h3>Pipeline Silver Stage Success</h3>
    <p><b>Task:</b> {context['task_instance'].task_id}</p>
    <p><b>DAG:</b> {context['task_instance'].dag_id}</p>
    <p><b>Success Time:</b> {context['ts']}</p>
    <p><b>Log URL:</b> <a href="{log_url}">Click here to view logs</a></p>
    """
    send_email(to=user_email, subject=subject, html_content=body)

def send_silver_failure_email(context):
    '''This func send failure notification if task on silver stage failed'''
    subject = f"Airflow Failure: {context['task_instance'].task_id}"
    log_url = context['task_instance'].log_url
    body = f"""
    <h3>Pipeline Silver Stage Failed</h3>
    <p><b>Task:</b> {context['task_instance'].task_id}</p>
    <p><b>DAG:</b> {context['task_instance'].dag_id}</p>
    <p><b>Error Time:</b> {context['ts']}</p>
    <p><b>Log URL:</b> <a href="{log_url}">Click here to view logs</a></p>
    """
    send_email(to=user_email, subject=subject, html_content=body)


@dag(dag_id="snowflake_wait_file",
     schedule="@daily",
     start_date=None, catchup=False,
     description="This dag waiting file in folder --DATA--",
     tags=["sensor_folder"])
def snowflake_wait_file():
    scan_folder = FileSensor(
        task_id="scan_folder",
        fs_conn_id="fs_default",
        filepath=file_path,
        poke_interval=5)

    @task.branch
    def scan_file(file_path):
        """Check if the file is empty or not"""
        with open(file_path, 'r', encoding='utf-8') as f:
            first_line = f.readline()
        if not first_line:
            return 'log_empty_file'
        else:
            return 'download_raw_in_stage'

    log = BashOperator(
            task_id='log_empty_file',
            bash_command='echo "The file is empty."'
        )

    @task
    def download_raw_in_stage():
        '''This func put csv file into snowflake'''
        snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake')
        results = snowflake_hook.run(f"PUT file://{file_path} @{stage_name} AUTO_COMPRESS=TRUE OVERWRITE=TRUE")
        print(f"Результат загрузки: {results}") 
    
    @task_group(group_id='medalion_structure')
    def transform_group():
        @task
        def raw_layer():
            '''This func ingest data in raw table'''
            hook = SnowflakeHook(snowflake_conn_id='snowflake')
            sql = f"""
            COPY INTO AIRLINE.PUBLIC.AIRLINE_BRONZE
            FROM (
                SELECT 
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 
                    $11, $12, $13, $14, $15, $16, $17, $18, 
                    CURRENT_TIMESTAMP() 
                FROM @{stage_name}
            )
            FILE_FORMAT = (FORMAT_NAME = '{file_format}')
            FORCE = TRUE;
            """
            result = hook.run(sql)
            print(f"Result of copy: {result}")
            return f'Raw layer done'

        # task with SLA on email   
        @task(on_success_callback=send_silver_success_email,
              on_failure_callback=send_silver_failure_email)
        def silver_layer(dummy):
            '''This task call procedure in snowflake for transform data and clean'''
            hook = SnowflakeHook(snowflake_conn_id='snowflake')
            sql = f"""CALL AIRLINE.SILVER.SP_LOAD_SILVER_LAYER();"""
            result = hook.run(sql)
            print(f"Result of procedure : {result}")
            return f'Silver layer done'
        
        @task
        def gold_layer(dummy):
            '''Check if find person with age < 18 then failed'''
            hook = SnowflakeHook(snowflake_conn_id='snowflake')
            sql="""SELECT COUNT(*) FROM AIRLINE.GOLD.FLIGHTS_ANALYTIC WHERE AGE < 18"""
            count_person = hook.get_first(sql)[0]
            print(count_person)
            if count_person > 0:
                raise ValueError(f"SECURITY BREACH: Find {count_person} rows with age < 18!")
            return "Check Passed"

        raw = raw_layer()
        silver = silver_layer(raw)
        gold = gold_layer(silver)     

    start = scan_folder
    branch = scan_file(file_path)
    start >> branch
    layers = transform_group()
    branch >> download_raw_in_stage() >> layers
    branch >> log

snowflake_wait_file()

