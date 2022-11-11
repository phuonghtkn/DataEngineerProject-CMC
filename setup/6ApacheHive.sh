#!/bin/bash

source DE1/bin/activate

sudo apt update
sudo apt install -y default-jre default-jdk ssh pdsh

#install hadoop first
wget https://dlcdn.apache.org/hadoop/common/hadoop-3.3.4/hadoop-3.3.4.tar.gz
mkdir -p hadoop
tar -xzf hadoop-3.3.4.tar.gz -C hadoop/.
rm -rf hadoop-3.3.4.tar.gz
cp hadoop/hadoop-3.3.4/etc/hadoop/hadoop-env.sh hadoop/hadoop-3.3.4/etc/hadoop/hadoop-env.sh.backup
echo "export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64" >> hadoop/hadoop-3.3.4/etc/hadoop/hadoop-env.sh

export PATH=$PATH:$(pwd)/hadoop/hadoop-3.3.4/bin


#install hadoop hive
wget https://dlcdn.apache.org/hive/hive-3.1.3/apache-hive-3.1.3-bin.tar.gz
mkdir -p hive
tar -xzf apache-hive-3.1.3-bin.tar.gz -C hive/.
rm -rf apache-hive-3.1.3-bin.tar.gz

export PATH=$PATH:$(pwd)/hive/apache-hive-3.1.3-bin/bin
cp hive/apache-hive-3.1.3-bin/bin/hive-config.sh hive/apache-hive-3.1.3-bin/bin/hive-config.sh.backup

echo "export HADOOP_HOME=$(pwd)/hadoop/hadoop-3.3.4" >> hive/apache-hive-3.1.3-bin/bin/hive-config.sh

mkdir -p hive/warehouse

#Create Hive directories in Hadoop
hadoop fs -mkdir $(pwd)/hive/warehouse/DE1
hadoop fs -chmod g+w $(pwd)/hive/warehouse/DE1

