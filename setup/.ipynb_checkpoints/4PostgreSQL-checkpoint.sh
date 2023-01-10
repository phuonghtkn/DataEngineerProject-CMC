#!/bin/bash


SCRIPT=$(readlink -f "$0")
SCRIPTPATH=$(dirname "$SCRIPT")

source $SCRIPTPATH/../DE1/bin/activate

sudo apt update
sudo apt install -y postgresql postgresql-contrib
sudo systemctl start postgresql.service
