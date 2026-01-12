from airflow.decorators import dag, task
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow import Dataset
from datetime import datetime
import pandas as pd
from dotenv import load_dotenv
import os


load_dotenv()
# from .env
final_file_path = os.getenv('FINAL_FILE')
final_file = Dataset(final_file_path)

@dag(
    dag_id="transit_to_mongo",
    start_date=None,
    schedule=[final_file], 
    catchup=False,
    tags=["sensor_folder"]
)
def second_dag():
    """Transit our data to mongoDB"""
    @task
    def data_to_mongo():
        df = pd.read_csv(final_file_path)
        for col in df.columns:
            if 'at' in col:
                try:
                    df[col] = pd.to_datetime(df[col])
                except Exception as E:
                    print(E)
        data = df.to_dict(orient='records')
        hook = MongoHook(mongo_conn_id='mongodb')
        client = hook.get_conn()
        db = client['airflow_tiktok']
        collection = db['tiktok']
        
        if data:
            result = collection.insert_many(data)
            print(f"Загрузка завершена!")
        else:
            print("Файл пуст, загружать нечего.")     
    

    data_to_mongo()

second_dag()