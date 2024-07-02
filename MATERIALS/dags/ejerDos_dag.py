# 2. Ejemplo de la ejecución de 2 rutinas Python que se llamen de forma secuencial (primero una y tan pronto termine la otra)
# 	2.1 Programar que el DAG construido se ejecute cada 5 min y tenga 3 reintentos cada 2 min en caso de fallo


from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta

def _print_tarea_1():
    print("Tarea 1")

def _print_tarea_2():
    print("Tarea 2")

with DAG (
            "ejerDos_dag",
            start_date=datetime(2024,1,1),
            schedule_interval = "*/5 * * * *",  #cada 5 minutos
            catchup=False,
            default_args={
                        "retries": 3,
                        "retry_delay": timedelta(minutes=2),
                        "email_on_failure": False,
            },
        )as dag:


    tarea_1 = PythonOperator (
        task_id = "tarea_1",
        python_callable= _print_tarea_1
    )

    tarea_2 = PythonOperator (
        task_id = "tarea_2",
        python_callable= _print_tarea_2
    )


    tarea_1 >> tarea_2