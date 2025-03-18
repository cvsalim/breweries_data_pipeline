from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.utils.email import send_email

# Function to notify by email in pipeline fail cases.
def notify_failure(context):
    task_instance = context['task_instance']
    subject = f"Task {task_instance.task_id} falhou na DAG {context['dag'].dag_id}"
    body = f"A task {task_instance.task_id} falhou. Verifique os logs para mais detalhes."
    send_email(to=['cvsalimoliveira@gmail.com'], subject=subject, html_content=body)  #Please edit it with your email

# Function to notify by email in pipeline success cases.
def notify_success():
    subject = "DAG pyspark_etl_pipeline executada com sucesso"
    body = "Todas as tasks da DAG pyspark_etl_pipeline foram concluídas com sucesso."
    send_email(to=['email@email.com'], subject=subject, html_content=body) # Please edit it with your email

# Default DAG args
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 18),
    'retries': 1,
    'on_failure_callback': notify_failure,
}

dag = DAG(
    'pyspark_etl_pipeline',
    default_args=default_args,
    description='DAG para pipeline ETL com PySpark',
    schedule_interval=None,
    catchup=False,
)

# Tasks
bronze_task = BashOperator(
    task_id='bronze_ingestion',
    bash_command='spark-submit --master local /opt/airflow/dags/bronze_ingestion.py',
    on_failure_callback=notify_failure,
    dag=dag,
)

silver_task = BashOperator(
    task_id='silver_ingestion',
    bash_command='spark-submit --master local /opt/airflow/dags/silver_ingestion.py',
    on_failure_callback=notify_failure,
    dag=dag,
)

gold_task = BashOperator(
    task_id='gold_ingestion',
    bash_command='spark-submit --master local /opt/airflow/dags/gold_ingestion.py',
    on_failure_callback=notify_failure,
    dag=dag,
)

success_task = PythonOperator(
    task_id='notify_success',
    python_callable=notify_success,
    dag=dag,
)

# Exectution
bronze_task >> silver_task >> gold_task >> success_task
