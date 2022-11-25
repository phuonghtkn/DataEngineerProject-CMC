# import the libraries 
from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

from airflow.operators.papermill_operator import PapermillOperator
# This makes scheduling easy
from airflow.utils.dates import days_ago 

#defining DAG arguments 
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'Phuong Hoang Tang Kim Nam',
    'start_date': days_ago(0),
    'email': ['phuonghtkn@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
} 

# define the DAG
dag = DAG(
    dag_id='customer_fact_generator',
    default_args=default_args,
    description='DAG DE1 data generator using jupyterlab',
    schedule_interval=timedelta(minutes=5),
)

with DAG(
    dag_id='customer_fact',
    default_args=default_args,
    schedule_interval='0 0 * * *',
    dagrun_timeout=timedelta(minutes=5)
) as dag:
    # [START howto_operator_papermill]
    run_this = PapermillOperator(
        task_id="customer_fact_generator",
        input_nb="/tmp/CustomerFactGenerator.ipynb"
    )