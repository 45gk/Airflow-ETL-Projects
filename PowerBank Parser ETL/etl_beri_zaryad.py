from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.sensors.http_sensor import HttpSensor
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import requests
import logging
from typing import List, Dict
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
import logging
import json
from datetime import datetime
import os
import sqlalchemy


logger = logging.getLogger(__name__)

API_BASE = "https://api.berizaryad.ru/locations"  # примерный базовый URL


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    # 'email': ['alerts@example.com'],
    # 'email_on_failure': True,
    # 'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}


def extract_beri_zaryad(**context) -> None:
    def fetch_api(city: str = "moscow") -> List[Dict]:
        """
        Запрашивает список всех станций 'Бери заряд' для указанного города
        и сохраняет результат в staging-таблицу PostgreSQL.
        """
        url = f"{API_BASE}"
        # params = {"city": city}
        headers = {
            "Accept": "application/json",
        # Если требуется авторизация:
        # "Authorization": "Bearer <YOUR_TOKEN>",
        }

        resp = requests.get(url, headers=headers, timeout=10) # , params=params
        resp.raise_for_status()
        data = resp.json()  # ожидаем: {"stations": [ {id, address, lat, lon, status, price}, … ]}

        return data
    
    data = fetch_api()
    logger.info(f"Загрузка завершена")

    context['ti'].xcom_push(key='extracted_data', value=data)


def transform_beri_zaryad(**context) -> None:
    extracted_data = context['ti'].xcom_pull(key='extracted_data', task_ids=['extract_beri_zaryad'])[0]

    table = pd.DataFrame(columns=['station_id','station_adress','latitude','longitude', 'time_location', 'description', 'deposit_amount', 'num_cells_total', 'machine_type' , 'vendor'])

    for station in extracted_data['data']:
        
        table.loc[len(table.index)] = [
            station['id'],
            station['attributes']['address'],
            station['attributes']['latitude'],
            station['attributes']['longitude'],
            station['attributes']['time-location'], 
            station['attributes']['description'], 
  
            station['attributes']['required-deposit-amount'] , 
            station['attributes']['num-cells-total'], 
            station['attributes']['machine-type'], 
            'berizaryad'
        ]
    
    logger.info(f"Трансформация завершена")
    # context['ti'].xcom_push(key='transformed_data', value=extracted_data)
    # Сохраняем в CSV во временную папку Airflow
    ts = datetime.utcnow().strftime('%Y%m%dT%H%M%S')
    path = f"/tmp/berizaryad_{ts}.csv"
    table.to_csv(path, index=False)

    # Передаём путь через XCom
    context['ti'].xcom_push(key='staging_csv_path', value=path)
    


def load_beri_zaryad(**context) -> None:
  

    csv_path = context['ti'].xcom_pull(
        task_ids='transform_beri_zaryad',
        key='staging_csv_path'
    )
    if not csv_path or not os.path.exists(csv_path):
        logger.error(f"CSV-файл не найден по пути {csv_path}")
        return


    # Читаем CSV в DataFrame

    df = pd.read_csv(csv_path)
    df = df.fillna(0)
    df = df.convert_dtypes()

    
    engine = sqlalchemy.create_engine("postgresql://postgres:123@host.docker.internal/postgres")
    df.to_sql(name='power_bank_vendors_info',con=engine, schema='public', if_exists='replace', index=False)
 


with DAG(
    'etl_beri_zaryad',
    default_args=default_args,
    description='ETL для Бери заряд',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    catchup=False,
    tags=['etl', 'berizaryad'],
) as dag:
    
    extract_task = PythonOperator(
        task_id='extract_beri_zaryad',
        python_callable=extract_beri_zaryad,
        provide_context=True,
    )

    tranform_task = PythonOperator(
        task_id='transform_beri_zaryad',
        python_callable=transform_beri_zaryad,
        provide_context=True,
    )

    load_task = PythonOperator(
        task_id='load_beri_zaryad',
        python_callable=load_beri_zaryad,
        provide_context=True,
    )

    extract_task >> tranform_task >> load_task
