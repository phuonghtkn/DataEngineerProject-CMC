#!/bin/bash


SCRIPT=$(readlink -f "$0")
# Absolute path this script is in, thus /home/user/bin
SCRIPTPATH=$(dirname "$SCRIPT")

echo $SCRIPTPATH > /tmp/project_de2_path

source $SCRIPTPATH/DE1/bin/activate

export AIRFLOW_HOME=$SCRIPTPATH/airflow

#Clean log
rm -rf $SCRIPTPATH/log/*


# #try to run jupyterlab
# if pgrep -x "jupyter-lab" >/dev/null
# then
#     echo "jupyter-lab was ran"
# else
#     jupyter-lab --ip=0.0.0.0 > $SCRIPTPATH/log/jupyter-lab.log 2> $SCRIPTPATH/log/jupyter-lab-error.log &
#     echo "jupyter-lab is running"
# fi

#try to run Airflow
if pgrep -x "airflow" >/dev/null
then
    echo "airflow was ran"
else
    airflow webserver -p 8080 > $SCRIPTPATH/log/airflow.log 2> $SCRIPTPATH/log/airflow-error.log &
    airflow scheduler -D > $SCRIPTPATH/log/airflow_scheduler.log 2> $SCRIPTPATH/log/airflow_scheduler.log &
    echo "airflow is running"
fi

#export PATH=$PATH:$SCRIPTPATH/hadoop/hadoop-3.3.4/bin:$SCRIPTPATH/hive/apache-hive-3.1.3-bin/bin
#run hive?

