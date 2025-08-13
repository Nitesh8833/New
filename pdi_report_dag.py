from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
from airflow.contrib.operators.gcp_secrets_manager import CloudSecretsManagerHook
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email': ['your-team@example.com'],
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def get_secrets():
    """Retrieves secrets from Google Cloud Secret Manager"""
    secrets_hook = CloudSecretsManagerHook()
    return {
        "DB_NAME": secrets_hook.get_secret("db-name"),
        "DB_USER": secrets_hook.get_secret("db-user"),
        "DB_PASSWORD": secrets_hook.get_secret("db-password"),
        "DB_HOST": secrets_hook.get_secret("db-host"),
        "DB_PORT": secrets_hook.get_secret("db-port"),
        "CERT_BUCKET": secrets_hook.get_secret("cert-bucket"),
        "CLIENT_CERT_PATH": secrets_hook.get_secret("client-cert-path"),
        "CLIENT_KEY_PATH": secrets_hook.get_secret("client-key-path"),
        "SERVER_CA_PATH": secrets_hook.get_secret("server-ca-path"),
        "OUTPUT_GS_URI": secrets_hook.get_secret("output-gs-uri"),
    }

def run_pdi_report(**kwargs):
    """Executes the PDI report pipeline with environment variables"""
    import os
    from pdi_report_pipeline import run_pipeline
    
    # Set environment variables from secrets
    secrets = kwargs['ti'].xcom_pull(task_ids='get_secrets')
    for key, value in secrets.items():
        os.environ[key] = value
    
    # Execute the pipeline
    return run_pipeline()

with DAG(
    'daily_pdi_report',
    default_args=default_args,
    description='Generates daily PDI report from Cloud SQL',
    schedule_interval='0 8 * * *',  # Daily at 8 AM
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=['pdi', 'reporting'],
) as dag:

    start = DummyOperator(task_id='start')
    
    get_secrets_task = PythonOperator(
        task_id='get_secrets',
        python_callable=get_secrets,
        provide_context=True,
    )
    
    run_report_task = PythonOperator(
        task_id='run_pdi_report',
        python_callable=run_pdi_report,
        provide_context=True,
    )
    
    end = DummyOperator(task_id='end')
    
    # Set up workflow
    start >> get_secrets_task >> run_report_task >> end
