from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging  # Import faltante

# Configuración básica de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def hello_world():
    """Función que ejecuta la lógica principal"""
    try:
        message = "Hola desde airflow dag"
        print(message)
        logger.info("✅ DAG ejecutado correctamente - Hola desde Airflow!")
        return True  # Retorno explícito importante
    except Exception as e:
        logger.error(f"❌ Error en la ejecución: {str(e)}")
        raise

with DAG(
    dag_id="hello_airflow",
    description="Dag prueba basico",
    schedule="@daily",
    start_date=datetime(2023, 4, 8),
    catchup=False,
    default_args={
        'owner': 'airflow',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        'depends_on_past': False,
    },
    tags=['testing', 'basic'],
    dagrun_timeout=timedelta(minutes=10),  # Timeout para el DAG completo
) as dag:
    
    tarea_saludo = PythonOperator(
        task_id="imprimir_mensaje",
        python_callable=hello_world,
        execution_timeout=timedelta(minutes=1),  # Timeout para la tarea
    )
    
    # No es necesario asignar a variable si no hay dependencias
    tarea_saludo