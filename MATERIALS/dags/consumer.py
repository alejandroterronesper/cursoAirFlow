from airflow import DAG, Dataset #importar dataset
from airflow.decorators import task
# from include.datasets import MY_FILE

from datetime import datetime

my_file = Dataset ("/tmp/my_file.txt") #usar el mismo dataset que el mismo que se actualiza
# my_file_2 = Dataset ("/tmp/my_file_2.txt")


with DAG (
    dag_id = "consumer",
    schedule = [my_file],
    start_date= datetime(2022, 1 , 1),
    catchup= False
): 
    
    @task
    def read_dataset():
        with open(my_file.uri, "r") as f:  # lee el dataset
            print(f.read())

    read_dataset()

