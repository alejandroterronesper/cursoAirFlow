from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


# importamos las variables de entorno 
from airflow.models import Variable


ENV = Variable.get("env")
ID = Variable.get("id")


TAGS = ["PtyhonDataFlow"]
DAG_ID = "parametros_dag"
DAG_DESCRIPCION = """ Probando dags de airflow """
DAG_SCHEDULE  = "0 9 * * *"
default_args = {
    "start_date": datetime (2022,1,1)
}
retries = 4
retry_delay = timedelta(minutes=5)

params = {"Manual": True, "Fecha": "2024-04-29"}

def execute_taks ():
    print("Env: " +  str(ENV)  +  " ID: " + str(ID))


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
 


# ahora se crean las tareas asociadas a ese DAG


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

start_task >> first_task >> end_task
