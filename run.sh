#!/bin/bash

source DE1/bin/activate

#Clean log
rm -rf log/*


#try to run jupyterlab
if pgrep -x "jupyter-lab" >/dev/null
then
    echo "jupyter-lab was ran"
else
    jupyter-lab > log/jupyter-lab.log 2> log/jupyter-lab-error.log &
    echo "jupyter-lab is running"
fi

#try to run Airflow
if [ -e log/user_created ]
then
    echo "Airflow user was created"
else
	echo "Airflow is creating"
    touch log/user_created
    airflow users create --username admin --firstname Phuong --lastname Hoang --role Admin --email phuonghtkn@gmail.com
fi

if pgrep -x "airflow" >/dev/null
then
    echo "airflow was ran"
else
    airflow db init
    airflow webserver -p 8080 > log/airflow.log 2> log/airflow-error.log &
    airflow scheduler -D > log/airflow_scheduler.log 2> log/airflow_scheduler.log &
    echo "airflow is running"
fi
