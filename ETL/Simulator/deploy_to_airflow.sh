#!/bin/bash

DAGSPATH=$(</tmp/project_de2_path)

DAGSPATH+='/airflow/dags'

mkdir -p $DAGSPATH
cp simulator.py $DAGSPATH
