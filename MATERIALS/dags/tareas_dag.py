from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


# importamos las variables de entorno 
from airflow.models import Variable


ENV = Variable.get("env")
ID = Variable.get("id")


TAGS = ["PtyhonDataFlow"]
DAG_ID = "tareas_dag"
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
        kwargs["ti"].xcom_push(key = "color", value = "Amarillo")
    else:
        kwargs["ti"].xcom_push(key = "color", value = "Rojo")

def context_task(**kwargs):
    ti = kwargs["ti"]
    color = ti.xcom_pull(task_ids = "first_task", key = "color")
    print(color)


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



    last_task = PythonOperator (
        task_id = "last_task",
        python_callable=context_task,
        retries= retries,
        retry_delay = retry_delay
    )




start_task >> first_task >> last_task >> end_task
