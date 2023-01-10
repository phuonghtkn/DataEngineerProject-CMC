from __future__ import annotations
# import the libraries 
from datetime import timedelta
from airflow.models.baseoperator import chain
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to write tasks!
from airflow.operators.bash_operator import BashOperator
# This makes scheduling easy
from airflow.utils.dates import days_ago 
from airflow.operators.python import BranchPythonOperator
from airflow.providers.amazon.aws.operators.s3 import (
    S3CopyObjectOperator,
    S3CreateBucketOperator,
    S3CreateObjectOperator,
    S3DeleteBucketOperator,
    S3DeleteBucketTaggingOperator,
    S3DeleteObjectsOperator,
    S3FileTransformOperator,
    S3GetBucketTaggingOperator,
    S3ListOperator,
    S3ListPrefixesOperator,
    S3PutBucketTaggingOperator,
)
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor, S3KeysUnchangedSensor
from airflow.utils.trigger_rule import TriggerRule
#from tests.system.providers.amazon.aws.utils import ENV_ID_KEY, SystemTestContextBuilder

import papermill as pm
#defining DAG arguments 
# You can override them on a per-task basis during operator initialization

project_path_file = open("/tmp/project_de2_path", "r")
project_path = project_path_file.readline().replace("\n", "")
project_path_file.close()


default_args = {
    'owner': 'Hoang Tang Kim Nam Phuong',
    'start_date': days_ago(0),
    'email': ['namphuonghoang.es@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
} 

with DAG(
    dag_id="De1-simulator",
    default_args=default_args,
    description='De1-simulator DAG', 
    schedule_interval=timedelta(minutes=5),
    tags=["DE1"],
) as dag:
    fact_customer_generator = PapermillOperator(
        task_id="run_customer_fact_generator",
        input_nb=project_path + "/data_generator/CustomerFactGenerator.ipynb",
        #output_nb="/tmp/out-{{ execution_date }}.ipynb",
        #parameters={"msgs": "Ran from Airflow at {{ execution_date }}!"},
    )
    fact_account_generator = PapermillOperator(
        task_id="run_account_fact_generator", 
        input_nb=project_path + "/data_generator/AccountFactGenerator.ipynb",
    )
    fact_transaction_generator = PapermillOperator(
        task_id="run_transaction_fact_generator",
        input_nb=project_path + "/data_generator/TransactionFactGenerator.ipynb",
    )
    
    
# task pipeline
fact_generator 