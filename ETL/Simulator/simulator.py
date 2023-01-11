from __future__ import annotations
from datetime import timedelta
from airflow.models.baseoperator import chain
from airflow import DAG
from airflow.utils.dates import days_ago 
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator
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
from airflow.providers.amazon.aws.hooks.s3 import S3Hook, provide_bucket_name, unify_bucket_name_and_key
import sys
project_path_file = open("/tmp/project_de2_path", "r")
project_path = project_path_file.readline().replace("\n", "")
project_path_file.close()
sys.path.insert(1, project_path+ '/data_generator/libs')
from generator import FakeAccountGenerator, FakeCustomerGenerator, FakeTransactionGenerator


default_args = {
    'owner': 'Hoang Tang Kim Nam Phuong',
    'start_date': days_ago(0),
    'email': ['namphuonghoang.es@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
} 

NUMBER_GENERATE_RECORD = 1000

def customer_generator():
    customer = FakeCustomerGenerator()
    customer.create_data(10)
    customer.to_file()
    customer.stop_spark()

def account_generator():
    account = FakeAccountGenerator()
    if not account.checkBankCustomerInitStatus():
        customer_generator()
        account.initConfigFile()
    account.create_data(20)
    account.to_file()
    account.stop_spark()

def transaction_generator():
    transaction = FakeTransactionGenerator()
    if not transaction.checkBankAccountInitStatus():
        customer_generator()
        transaction.initConfigFile()
    transaction.create_data(100000)
    transaction.to_file()
    transaction.stop_spark()


def create_s3_path(bucket_name, fact_type, year, month, day):
    hook = S3Hook()
    bucket = hook.get_bucket(bucket_name)
    s3_path = fact_type + '/' + year + '/' + month + '/' + day
    bucket.put_object(Key=s3_path)

with DAG(
    dag_id="De1-simulator",
    default_args=default_args,
    description='De1-simulator DAG', 
    schedule_interval=timedelta(minutes=5),
    tags=["DE1"],
) as dag:
    customer_generator_task = PythonOperator(
        task_id='customer_generate',
        python_callable= customer_generator,
        dag=dag,
    )
    account_generator_task = PythonOperator(
        task_id='account_generate',
        python_callable= account_generator,
        dag=dag,
    )
    transaction_generator_task = PythonOperator(
        task_id='transaction_generate',
        python_callable= transaction_generator,
        dag=dag,
    )





    
    
    
    
# # task pipeline
customer_generator_task>>account_generator_task>>transaction_generator_task