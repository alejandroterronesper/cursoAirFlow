from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta 

# operadores de tipo python  
from airflow.operators.python import PythonOperator


dag_owner = 'pool_dag'

default_args = {'owner': dag_owner,
        'depends_on_past': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5)
        }

with DAG(dag_id='pool_dag',
        default_args=default_args,
        description='pool_dag',
        start_date=datetime(2024, 5, 26),
        schedule_interval='0 9 * * *',
        catchup=False,
        tags=['pool_dag']
):

    start = EmptyOperator(task_id='start')


    end = EmptyOperator(task_id='end')


    # tarea de tipo python
    python_task = python_task = PythonOperator(
        task_id="python_task",
        python_callable=lambda: print('Hi from python operator'),
        pool = "pool_dag"
    )

    python_task2 = python_task = PythonOperator(
        task_id="python_task2",
        python_callable=lambda: print('Hi from python operator'),
        pool = "pool_dag"
    )

    python_task3 = python_task = PythonOperator(
        task_id="python_task3",
        python_callable=lambda: print('Hi from python operator'),
        pool = "pool_dag"
    )

    python_task4 = python_task = PythonOperator(
        task_id="python_task4",
        python_callable=lambda: print('Hi from python operator'),
        pool = "pool_dag"
    )    

    python_task5 = python_task = PythonOperator(
        task_id="python_task5",
        python_callable=lambda: print('Hi from python operator'),
        pool = "pool_dag"
    )

    python_task6 = python_task = PythonOperator(
        task_id="python_task6",
        python_callable=lambda: print('Hi from python operator'),
        pool = "pool_dag"
    )

    start >> [python_task, python_task2, python_task3, python_task4, 
              python_task5, python_task6]  >> end