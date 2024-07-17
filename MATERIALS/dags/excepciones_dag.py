from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException, AirflowException, AirflowTaskTimeout
import time



# importamos las variables de entorno 
from airflow.models import Variable


ENV = Variable.get("env")
ID = Variable.get("id")


TAGS = ["PtyhonDataFlow"]
DAG_ID = "excepciones_dag"
DAG_DESCRIPCION = """ Usando las tareas en airflow """
DAG_SCHEDULE  = "0 9 * * *"

default_args = {
    "start_date": datetime (2022,1,1)
}


retries = 4
retry_delay = timedelta(minutes=5)

params = {
            "Manual": False
        }

def execute_taks (**kwargs):
    print(kwargs)
    params = kwargs.get("params", {})
    manual = params.get("Manual", False)

    if manual: 
        timeout = 10
        start_time = time.time()

        while True:
            elapsed_time = time.time() - start_time
            if elapsed_time > timeout:
                raise AirflowTaskTimeout ("La tarea ha execido  el tiempo de espera especificado")
            time.sleep(1)
        # raise AirflowException ("La tarea ha fallado")
        # raise AirflowSkipException("La tarea ha sido omitida")




dag = DAG (
    dag_id = DAG_ID,
    description= DAG_DESCRIPCION,
    catchup= False, #hace que el dag se ponga al día
    schedule_interval=DAG_SCHEDULE,
    max_active_runs=1, #maxima canticdad de ejecuciones
    dagrun_timeout=200000,
    default_args=default_args,
    tags=TAGS,
    params= params
)
 


with dag as dag:
    
    start_task = EmptyOperator (
        task_id="start_task"
    )


    end_task = EmptyOperator (
        task_id="end_task"
    )


    first_task = PythonOperator (
        task_id = "first_task",
        python_callable=execute_taks,
        retries= retries,
        retry_delay = retry_delay
    )








start_task >> first_task  >> end_task
