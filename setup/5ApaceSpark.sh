#!/bin/bash

source DE1/bin/activate

sudo apt update
sudo apt install default-jdk scala git -y
pip install pandas pyarrow py4j numpy
pip install pyspark
