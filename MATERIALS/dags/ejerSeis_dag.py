# 6. Ejemplo de la ejecución de 2 rutinas Python que se ejecuten en paralelo 
# y cuando ambas hayan terminado se ejecute la rutina 3 (Java o Bash)
# 6.1 Incluir la llamda a un job spark que se ejecute tan ptonto termine la rutina 3 
# (programada en Java o python)

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from datetime import datetime, timedelta


def _rutina_1():
    print("rutina 1")

def _rutina_2():
    print("rutina 2")


with DAG (
    "ejerSeis_dag",
    start_date= datetime (2024,1,1),
    schedule_interval= "@daily",
    catchup= False
) as dag: 



    rutina_1 = PythonOperator (
        task_id = "rutina_1",
        python_callable= _rutina_1
    )

    rutina_2 = PythonOperator (
        task_id = "rutina_2",
        python_callable= _rutina_2
    )


    rutina_3 = BashOperator (
        task_id = "rutina_3",
        bash_command= "echo 'tarea 3 realizada porque se ha hecho la 1 y la 2'"
    )


    [rutina_1, rutina_2] >> rutina_3