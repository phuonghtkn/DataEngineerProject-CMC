from __future__ import annotations
from datetime import timedelta
from airflow.models.baseoperator import chain
from airflow import DAG
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

# with DAG(
#     dag_id="De1-simulator",
#     default_args=default_args,
#     description='De1-simulator DAG', 
#     schedule_interval=timedelta(minutes=5),
#     tags=["DE1"],
# ) as dag:
    
    
    
# # task pipeline
# fact_generator 