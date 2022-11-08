#!/bin/bash

sudo apt install -y software-properties-common
sudo apt-add-repository universe
sudo apt update
sudo apt install -y vim python3 
sudo apt install -y pip 
#sudo apt install -y python3-pip
sudo apt install -y python-setuptools python3-pip
sudo apt install -y libmysqlclient-dev libssl-dev libkrb5-dev
sudo apt install -y nodejs npm
pip3 install virtualenv
#sudo apt install -y python3-virtualenv
virtualenv -p python3 ../DE1

