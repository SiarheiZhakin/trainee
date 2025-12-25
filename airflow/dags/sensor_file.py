from airflow.decorators import task, dag, task_group
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator
from airflow.utils.edgemodifier import Label
from airflow import Dataset
from dotenv import load_dotenv
import os
import pandas as pd

load_dotenv()
path_to_folder = os.getenv('PATH_FOLDER')
file = os.getenv('FILE')
final_file = os.getenv('FINAL_FILE')
final_file = Dataset(final_file)
file_path = f'{path_to_folder}{file}'


# file_path = "/opt/airflow/data/tiktok_google_play_reviews.csv"
@dag(dag_id="waiting_files",
     schedule="@daily",
     start_date=None, catchup=False,
     description="This dag waiting file in folder --DATA--",
     tags=["sensor_folder"])
def waiting_files():
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
            return 'process_file_task'

    @task
    def process_file_task():
        """Task is executed if the file is not empty"""
        print('file not empty')
    #log if the file is empty
    log = BashOperator(
            task_id='log_empty_file',
            bash_command='echo "The file is empty."'
        )
   

    @task_group(group_id='transform_data')
    def transform_task_group(input_file):

        @task
        def replace_nulls(path):
            df = pd.read_csv(path, engine='pyarrow')
            replace = df.fillna('-', inplace=True)
            df.to_csv(f"{path_to_folder}replace_nulls.csv", index=False)
            path = "data/replace_nulls.csv"
            return path
            
        @task
        def sort_data(path):
            df = pd.read_csv(path, engine='pyarrow')
            sort = df.sort_values('at')
            df.to_csv(f"{path_to_folder}sort_data.csv", index=False)
            path = "data/sort_data.csv"
            return path

        @task(outlets=[final_file])
        def only_text(path):
            df = pd.read_csv(path, engine='pyarrow')
            regex_pattern = r'[^a-zA-Zа-яА-ЯёЁ\s\.,!;:-]'
            df['content'] = df['content'].replace(regex_pattern, '', regex=True)
            df.to_csv(f"{path_to_folder}final_result.csv", index=False)


        path_to_sort = replace_nulls(input_file)
        before_final_save = sort_data(path_to_sort)
        only_text(before_final_save)


    start = scan_folder
    branch = scan_file(file_path)
    transform_file = process_file_task()
    start >> branch
    branch >> Label("File not empty") >> transform_file
    branch >> Label("Empty file") >> log
    group_execution = transform_task_group(file_path)
    transform_file >> Label("To transform") >> group_execution

waiting_files()

