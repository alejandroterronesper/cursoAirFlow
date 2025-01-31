from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator

from datetime import datetime

"""
Todas estas funciones son de tipo callable 

ti-> task instance object

"""
def _t1(ti):
    ti.xcom_push(key="my_key", value=42) #envia el dato


def _t2(ti):
    print(ti.xcom_pull(key="my_key", task_ids="t1")) #extrae el dato con PUSHç

def _branch(ti):
    value = ti.xcom_pull(key = "my_key", task_ids ="t1")

    if (value == 42):
        return "t2" #ejecutamos task t2
    
    return "t3"
    
with DAG("xcom_dag", 
        start_date=datetime(2022, 1, 1), 
        schedule_interval='@daily',
        catchup=False) as dag:

    t1 = PythonOperator(
        task_id='t1',
        python_callable=_t1
    )

    branch = BranchPythonOperator(
        task_id = "branch",
        python_callable = _branch
    )

    t2 = PythonOperator(
        task_id='t2',
        python_callable=_t2
    )

    t3 = BashOperator(
        task_id='t3',
        bash_command="echo ''"
    )

    t4 = BashOperator(
        task_id='t4',
        bash_command="echo ''"
    )

    t1 >> branch >> [t2,t3] >> t4 #el t3 se salta porque se cumple la condición del if