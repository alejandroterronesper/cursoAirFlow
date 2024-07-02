from airflow import DAG
from airflow.decorators import task, dag
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime

@dag(
    start_date=datetime(2021, 1, 1),
    schedule_interval="@daily",
    catchup=False
)
def docker_dag():
        
    @task()
    def t1():
        pass

    t2 = DockerOperator(
        task_id="t2",
        api_version = "auto",
        container_name="task_t2",
        image="stock_image:v1.0.0",
        command='python3 stock_data.py',
        docker_url='tcp://localhost:2375',
        network_mode='bridge',
        xcom_all = True,
        # retrieve_output=True,
        # retrieve_output_path = "/tmp/script.out",
        # auto_remove = True,
        # mount_tmp_dir = True, 
        # mount_tmp_dir = False,
        # mounts = [
        #     Mount(source = "")
        # ]
    )

    t1() >> t2

dag_instance = docker_dag()

# seguir con video 98

# from airflow.decorators import task, dag
# from airflow.providers.docker.operators.docker import DockerOperator

# from datetime import datetime

# @dag(
#       start_date = datetime(2021,1,1),
#       schedule_interval = "@daily",
#       catchup = False
#     )

# def docker_dag():
    
        
#     @task()
#     def t1():
#         pass

#     t2 = DockerOperator (
#         task_id = "t2",
#         image="stock_image:v1.0.0",
#         command = 'python3 stock_data.py',
#         docker_url = "unix://var/run/docker.sock",
#         network_mode = "bridge"
#     )

#     t1() >> t2


# dag = docker_dag()

# me quedo en video 97 12:32
