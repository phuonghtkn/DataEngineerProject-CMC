#!/bin/bash


SCRIPT=$(readlink -f "$0")
# Absolute path this script is in, thus /home/user/bin
SCRIPTPATH=$(dirname "$SCRIPT")

echo $SCRIPTPATH > /tmp/project_de2_path

source $SCRIPTPATH/DE1/bin/activate

export AIRFLOW_HOME=$SCRIPTPATH/airflow

#Clean log
rm -rf $SCRIPTPATH/log/*

touch log/user_created
airflow users create --username admin --firstname Phuong --lastname Hoang --role Admin --email phuonghtkn@gmail.com
airflow db init

