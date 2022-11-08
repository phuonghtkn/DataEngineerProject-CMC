#!/bin/bash

source DE1/bin/activate

sudo apt update
sudo apt install -y postgresql postgresql-contrib
sudo systemctl start postgresql.service
