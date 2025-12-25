from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from datetime import datetime

def check_mongo_connection():
    # Создаем Hook, который использует connection_id из настроек Airflow
    hook = MongoHook(mongo_conn_id='mongodb')
    
    # Вместо простого get_conn() попробуем проверить авторизацию напрямую
    client = hook.get_conn()
    
    # Явно указываем базу admin для пинга
    db = client.admin 
    db.command('ping')
    print("Успешно: Соединение с MongoDB установлено!")
with DAG(
    dag_id='check_mongo_db_connection',
    start_date=None,
    catchup=False
) as dag:

    test_connection = PythonOperator(
        task_id='test_mongo_ping',
        python_callable=check_mongo_connection
    )


    
