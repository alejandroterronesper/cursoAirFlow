# 5. Ejemplo de la ejecución de 2 rutinas Python que se ejecuten en paralelo
# y cuando ambas hayan terminado se ejecute la rutina 3 (Python)
# 5.2 Hacer que este pipeline ya sea con un único DAG
#  o con varios se ejecute todos los 
# días martes a las 10 am y los días 15 y 30 de cada mes a las 10:30 am


from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta


def _print_tarea_1():
        print("Tarea 1 realizada")


def _print_tarea_2():
        print("Tarea 2 realizada")

def _print_tarea_3():
        print("Tarea 3 realizada porque la 1 y la 2 han sido realizadas")


with DAG (
            "ejerCinco_dag",
            start_date = datetime (2024,1,1),
            schedule_interval="0 10 * * 2",
            catchup=False
        ) as dag:
    

        tarea_1 = PythonOperator (
                task_id = "tarea_1",
                python_callable= _print_tarea_1,
                # schedule_interval = "30 15 10,15 * *"
        )

        tarea_2 = PythonOperator (
                task_id = "tarea_2",
                python_callable= _print_tarea_2,
                # schedule_interval = "30 15 10,15 * *"

        )

        tarea_3 = PythonOperator (
                task_id = "tarea_3",
                python_callable= _print_tarea_3,
        )



        [tarea_1, tarea_2] >> tarea_3