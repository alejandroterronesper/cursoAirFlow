from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException, AirflowException, AirflowTaskTimeout
import time
from airflow.utils.trigger_rule import TriggerRule


# importamos las variables de entorno 
from airflow.models import Variable


ENV = Variable.get("env")
ID = Variable.get("id")


TAGS = ["PtyhonDataFlow"]
DAG_ID = "trigger_dag"
DAG_DESCRIPCION = """ Usando las tareas en airflow """
DAG_SCHEDULE  = "0 9 * * *"

default_args = {
    "start_date": datetime (2022,1,1)
}



params = {
            "Manual": False
        }

def execute_taks (**kwargs):
    # print(kwargs)
    params = kwargs.get("params", {})
    manual = params.get("Manual", False)

    if manual: 
        raise AirflowException ("La tarea ha fallado")

  
            
def second_tasks(**kwargs):
    print("Second Task")
    # params = kwargs.get("params", {})
    # manual = params.get("Manual", False)

    # if manual: 
    #     raise AirflowException ("La tarea ha fallado")


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
        task_id="end_task",
        trigger_rule=TriggerRule.ONE_SUCCESS
        # trigger_rule=TriggerRule.ALL_FAILED
        # trigger_rule=TriggerRule.ALL_SUCCESS
    )


    first_task = PythonOperator (
        task_id = "first_task",
        python_callable=execute_taks,
        provide_context = True
    )

    second_task = PythonOperator (
        task_id = "second_task",
        python_callable=execute_taks,
        provide_context=True
    )






start_task >> [first_task, second_task]  >> end_task
