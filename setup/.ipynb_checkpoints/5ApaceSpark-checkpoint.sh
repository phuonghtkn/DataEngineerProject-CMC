#!/bin/bash

SCRIPT=$(readlink -f "$0")
SCRIPTPATH=$(dirname "$SCRIPT")

source $SCRIPTPATH/../DE1/bin/activate

sudo apt update
sudo apt install default-jdk scala git -y
pip install pandas pyarrow py4j numpy
pip install pyspark
