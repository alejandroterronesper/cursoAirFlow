# 4. Ejemplo de la ejecución de 2 rutinas bash que se ejecuten en paralelo 
# y cuando ambas hayan terminado se ejecute la rutina 3 (bash)
# 4.1 Programar que el DAG construido se ejecute una sola vez al día
# (a cualquier hora) solo los días lunes, miercoles y viernes con 3 reintentos cada 15 min en caso de fallo


from airflow import DAG
from airflow.operators.bash import BashOperator

from datetime import datetime, timedelta


with DAG (
        "ejerCuatro_dag",
        start_date = datetime (2024,1,1),
        schedule_interval="0 0 * * 1,2,3",
        catchup  = False,
        default_args={
            "retries": 3, # número de intentos
            "retry_delay": timedelta(minutes=15), #15 minutos cada tres intentos
            "email_on_failure": False,
        },
    ) as dag:


    rutina_1 = BashOperator (
        task_id = "rutina_1",
        bash_command= "echo 'se ha realizado la tarea 1'"
    )

    rutina_2 = BashOperator (
        task_id = "rutina_2",
        bash_command= "echo 'se ha realizado la tarea 3'"
    )


    rutina_3 = BashOperator (
        task_id = "rutina_3",
        bash_command= "echo 'se ha realizado la tarea 3 porque la 1 y la 2 se han realizado'"
    )


    [rutina_1, rutina_2] >> rutina_3




