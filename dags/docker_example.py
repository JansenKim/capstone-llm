# from datetime import datetime, timedelta
# from airflow import DAG
# from airflow.operators.bash import BashOperator
# from airflow.providers.docker.operators.docker import DockerOperator
# from airflow.operators.empty import EmptyOperator
# from airflow.models import Variable

# default_args = {
#     "owner": "airflow",
#     "description": "Use of the DockerOperator",
#     "depend_on_past": False,
#     "start_date": datetime(2021, 5, 1),
#     "email_on_failure": False,
#     "email_on_retry": False,
#     "retries": 1,
#     "retry_delay": timedelta(minutes=5),
# }

# with DAG(
#     "docker_operator_dag",
#     default_args=default_args,
#     schedule="5 * * * *",
#     catchup=False,
# ) as dag:
#     start_dag = EmptyOperator(task_id="start_dag")

#     end_dag = EmptyOperator(task_id="end_dag")

#     t1 = BashOperator(task_id="print_current_date", bash_command="date")

#     t2 = DockerOperator(
#         task_id="clean_function",
#         image="my-capstone-llm:v2",
#         container_name="my-capstone-llm_clean_task",
#         api_version="auto",
#         auto_remove="force",
#         command=["python3", "-m",  "capstonellm.tasks.clean"],
#         docker_url="unix://var/run/docker.sock",
#         network_mode="bridge",
#         environment={
#             "AWS_ACCESS_KEY_ID": Variable.get('AWS_ACCESS_KEY_ID'),
#             "AWS_SECRET_ACCESS_KEY": Variable.get('AWS_SECRET_ACCESS_KEY'),
#         },
#     )

#     t4 = BashOperator(task_id="print_hello", bash_command='echo "hello world"')

#     start_dag >> t1

#     t1 >> t2 >> t4
#     t1 >> t4

#     t4 >> end_dag
