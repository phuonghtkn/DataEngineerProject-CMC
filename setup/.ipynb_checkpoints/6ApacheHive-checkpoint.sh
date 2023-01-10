#!/bin/bash

SCRIPT=$(readlink -f "$0")
SCRIPTPATH=$(dirname "$SCRIPT")

source $SCRIPTPATH/../DE1/bin/activate

sudo apt update
sudo apt install -y default-jre default-jdk ssh pdsh

#install hadoop first
wget https://dlcdn.apache.org/hadoop/common/hadoop-3.3.4/hadoop-3.3.4.tar.gz
mkdir -p $SCRIPTPATH/../hadoop
tar -xzf $SCRIPTPATH/hadoop-3.3.4.tar.gz -C $SCRIPTPATH/../hadoop/.
rm -rf $SCRIPTPATH/hadoop-3.3.4.tar.gz
cp $SCRIPTPATH/../hadoop/hadoop-3.3.4/etc/hadoop/hadoop-env.sh $SCRIPTPATH/../hadoop/hadoop-3.3.4/etc/hadoop/hadoop-env.sh.backup
echo "export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64" >> $SCRIPTPATH/../hadoop/hadoop-3.3.4/etc/hadoop/hadoop-env.sh

export PATH=$PATH:$SCRIPTPATH/../hadoop/hadoop-3.3.4/bin


#install hadoop hive
wget https://dlcdn.apache.org/hive/hive-3.1.3/apache-hive-3.1.3-bin.tar.gz
mkdir -p $SCRIPTPATH/../hive
tar -xzf $SCRIPTPATH/apache-hive-3.1.3-bin.tar.gz -C $SCRIPTPATH/../hive/.
rm -rf $SCRIPTPATH/apache-hive-3.1.3-bin.tar.gz

export PATH=$PATH:$SCRIPTPATH/../hive/apache-hive-3.1.3-bin/bin
cp $SCRIPTPATH/../hive/apache-hive-3.1.3-bin/bin/hive-config.sh $SCRIPTPATH/../hive/apache-hive-3.1.3-bin/bin/hive-config.sh.backup

echo "export HADOOP_HOME=$(pwd)/hadoop/hadoop-3.3.4" >> $SCRIPTPATH/../hive/apache-hive-3.1.3-bin/bin/hive-config.sh

mkdir -p $SCRIPTPATH/../hive/warehouse

#Create Hive directories in Hadoop
hadoop fs -mkdir $SCRIPTPATH/../hive/warehouse/DE1
hadoop fs -chmod g+w $SCRIPTPATH/../hive/warehouse/DE1

