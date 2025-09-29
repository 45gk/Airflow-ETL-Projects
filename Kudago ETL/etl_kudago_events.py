import requests
import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
import os
import sqlalchemy
import pandas as pd


city_tags = ['kzn', 'msk']


logger = logging.getLogger(__name__)


default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'start_date': datetime(2025, 9, 28),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}



    

# Операция поглощения из API


def extract_data(city_tags: list,**context) -> None:
    for tag in city_tags:
        req = requests.get(url='https://kudago.com/public-api/v1.4/events/', params={
                                                                                        'page_size': 100,
                                                                                        'location': tag,
                                                                                        'actual_since': '2025-09-22', # убрать хардкод
                                                                                        'fields':'id,title,short_title,place,description,body_text,categories,age_restriction,price,is_free,participants'
                                                                                        # убрано: ,dates
                                                                                    })
        data = req.json()
        context['ti'].xcom_push(key=f'extracted_data_{tag}', value=data)
    


# Операция трансформации


def transform_events(city_tags: list, **context) -> None:
    now = datetime.now()
# ,'latitude' и , 'longitude' можно получить по свзяи id; , 'dates' - пока убрал
    table = pd.DataFrame(columns=['title', 'place', 'age_restriction', 'price', 'is_free','description', 'body_text', 'categories', 'participants' ,'city', 'airflow_loading'])
    for tag in city_tags:
        extracted_data = context['ti'].xcom_pull(key=f'extracted_data_{tag}', task_ids=['extract_kudago'])[0]

        
        
        for place in extracted_data["results"]:
            if place['place'] is not None:
                table.loc[len(table.index)] = [
                    place['title'],
                    place['place']['id'],
                    place['age_restriction'],
                    place['price'],
                    place['is_free'],
                    place["description"],
                    place['body_text'],
                    place['categories'],
                    place['participants'],
        
                    tag,
                    now
                ]
            else:
                table.loc[len(table.index)] = [
                    place['title'],
                    'No',
                    place['age_restriction'],
                    place['price'],
                    place['is_free'],
                    place["description"],
                    place['body_text'],
                    place['categories'],
                    place['participants'],
        
                    tag,
                    now
                ]


    logger.info(f"Трансформация завершена")
        # context['ti'].xcom_push(key='transformed_data', value=extracted_data)
        # Сохраняем в CSV во временную папку Airflow
    ts = datetime.utcnow().strftime('%Y%m%dT%H%M%S')
    path = f"/tmp/kudago_event_{ts}.csv"
    table.to_csv(path, index=False)

    # Передаём путь через XCom
    context['ti'].xcom_push(key='staging_csv_path', value=path)


# Операция загрузки в БД


def load_events(**context) -> None:
    csv_path = context['ti'].xcom_pull(
        task_ids='transform_kudago',
        key='staging_csv_path'
    )
    if not csv_path or not os.path.exists(csv_path):
        logger.error(f"CSV-файл не найден по пути {csv_path}")
        return


    # Читаем CSV в DataFrame

    df = pd.read_csv(csv_path)
    df = df.fillna(0)
    df = df.convert_dtypes()

    
    engine = sqlalchemy.create_engine("postgresql://postgres:123@host.docker.internal/walky_test")
    df.to_sql(name='kudago_events',con=engine, schema='public', if_exists='replace', index=False)



# Сам ДАГ


with DAG(
    'etl_kudago_events',
    default_args=default_args,
    description='ETL для Kudago',
    schedule='@daily',
    start_date = datetime.now() - timedelta(days=1),
    catchup=False,
    tags=['etl', 'kudago'],
) as dag:
    
    ectract_task = PythonOperator(
        task_id='extract_kudago',
        python_callable=extract_data,
        op_args=[city_tags],
    )

    transform_task = PythonOperator(
        task_id='transform_kudago',
        python_callable=transform_events,
        op_args=[city_tags],
    )

    load_task = PythonOperator(
        task_id='load_kudago',
        python_callable=load_events,

    )

    ectract_task >> transform_task >> load_task
