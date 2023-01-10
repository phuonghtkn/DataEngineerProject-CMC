#!/bin/bash

SCRIPT=$(readlink -f "$0")
SCRIPTPATH=$(dirname "$SCRIPT")

source $SCRIPTPATH/../DE1/bin/activate


pip install papermill
pip install apache-airflow-providers-papermill