# 1. Ejemplo de 2 rutinas bash (por ejemplos script de bash o sh) que se llamen de forma secuencial (primero una y tan pronto termine la otra)
# 	1.1 Programar que el DAG construido se ejecute cada 10 min


from airflow import DAG
from airflow.operators.bash import BashOperator


from datetime import datetime


with DAG ("ejerUno_dag",
          start_date=datetime(2022,1,1),
          schedule_interval = "*/10 * * * *",  #cada 10 minutos
          catchup = False
          ) as dag:
    


        rutina_1 = BashOperator (
            task_id = "rutina_1",
            bash_command= " echo 'tarea 1 hecha'"
        )

        rutina_2 = BashOperator (
            task_id = "rutina_2",
            bash_command= " echo 'tarea 2 hecha'"
        )


        rutina_1 >> rutina_2
