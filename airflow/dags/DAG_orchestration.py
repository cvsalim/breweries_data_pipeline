from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import papermill as pm
import logging
import os
import requests

def execute_notebook(notebook_path, output_path):
    try:
        pm.execute_notebook(
            notebook_path,
            output_path
        )
    except Exception as e:
        logging.error(f"Execution failed for {notebook_path}: {str(e)}")
        send_slack_alert(f"Execution failed for {notebook_path}")
        raise

def send_slack_alert(message):
    slack_webhook_url = os.getenv("SLACK_WEBHOOK_URL")  # Set your Slack webhook URL as an env variable
    if slack_webhook_url:
        requests.post(slack_webhook_url, json={"text": message})
    else:
        logging.warning("Slack webhook URL not set")

def run_bronze():
    execute_notebook("/mnt/data/bronze_ingestion.ipynb", "/mnt/data/bronze_output.ipynb")

def run_silver():
    execute_notebook("/mnt/data/silver_ingestion.ipynb", "/mnt/data/silver_output.ipynb")

def run_gold():
    execute_notebook("/mnt/data/gold_ingestion.ipynb", "/mnt/data/gold_output.ipynb")

def run_quality_checks():
    logging.info("Running data quality checks")
    # Add quality check logic here (e.g., verifying row counts, missing values, etc.)

def notify_success():
    send_slack_alert("Data pipeline executed successfully!")

# Define DAG
with DAG(
    "medalhao_data_pipeline",
    default_args={
        "owner": "airflow",
        "retries": 2,
    },
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
) as dag:

    task_bronze = PythonOperator(
        task_id="bronze_layer",
        python_callable=run_bronze,
    )

    task_silver = PythonOperator(
        task_id="silver_layer",
        python_callable=run_silver,
    )

    task_gold = PythonOperator(
        task_id="gold_layer",
        python_callable=run_gold,
    )

    task_quality = PythonOperator(
        task_id="quality_checks",
        python_callable=run_quality_checks,
    )

    task_notify = PythonOperator(
        task_id="notify_success",
        python_callable=notify_success,
    )

    # Define task dependencies
    task_bronze >> task_silver >> task_gold >> task_quality >> task_notify
