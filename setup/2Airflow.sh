#!/bin/bash

cd ../
source DE1/bin/activate
mkdir -p airflow
export AIRFLOW_HOME=$PWD/airflow
pip3 install apache-airflow
pip3 install typing_extensions
pip3 install apache-airflow[cncf.kubernetes]
cd setup
