from airflow import DAG  # Corrección en el import
from airflow.operators.python import PythonOperator  # Ruta corregida
from datetime import datetime, timedelta
import logging

def extract(**kwargs):
    datos = ["manzana", "banana", "cereza"]
    logging.info(f"Datos extraídos: {datos}")
    return datos

def transform(**kwargs):
    ti = kwargs['ti']
    datos = ti.xcom_pull(task_ids='extraer_datos')
    datos_transformados = [x.upper() for x in datos]
    logging.info(f"Datos transformados: {datos_transformados}")
    return datos_transformados

def load(**kwargs):
    ti = kwargs['ti']
    datos_finales = ti.xcom_pull(task_ids='transformar_datos')
    logging.info(f"Datos cargados: {datos_finales}")
    return True  # Retorno añadido

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'depends_on_past': False,
}

with DAG(
    dag_id='etl_basico',
    description='ETL básico con PythonOperator',
    schedule='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args=default_args,
    dagrun_timeout=timedelta(minutes=30),  # Timeout añadido
    tags=['ETL', 'basico'],
) as dag:

    t1 = PythonOperator(
        task_id='extraer_datos',
        python_callable=extract,
    )

    t2 = PythonOperator(
        task_id='transformar_datos',
        python_callable=transform,
    )

    t3 = PythonOperator(
        task_id='cargar_datos',
        python_callable=load,
    )

    t1 >> t2 >> t3