#!/bin/bash

SCRIPT=$(readlink -f "$0")
SCRIPTPATH=$(dirname "$SCRIPT")

source $SCRIPTPATH/../DE1/bin/activate
mkdir -p airflow
export AIRFLOW_HOME=$SCRIPTPATH/../airflow
pip3 install apache-airflow
pip3 install typing_extensions
pip3 install apache-airflow[cncf.kubernetes]
