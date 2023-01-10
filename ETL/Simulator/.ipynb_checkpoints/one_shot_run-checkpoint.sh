#!/bin/bash

SCRIPT=$(readlink -f "$0")
# Absolute path this script is in, thus /home/user/bin
SCRIPTPATH=$(dirname "$SCRIPT")

source $SCRIPTPATH/../../DE1/bin/activate

pip install 'apache-airflow[amazon]'
pip install papermill
pip install apache-airflow-providers-papermill
pip install apache-airflow-providers-amazon