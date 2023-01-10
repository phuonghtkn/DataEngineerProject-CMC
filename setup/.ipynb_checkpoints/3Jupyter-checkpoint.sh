#!/bin/bash

SCRIPT=$(readlink -f "$0")
SCRIPTPATH=$(dirname "$SCRIPT")

source $SCRIPTPATH/../DE1/bin/activate

#check to install jupyter
if pip --disable-pip-version-check list | grep -w jupyter;
then
    echo "jupyter installed"
else
    pip3 install jupyter
    echo "jupyter is new installed"
fi

#check to install jupyterlab
if pip --disable-pip-version-check list | grep -w jupyterlab;
then
    echo "jupyterlab installed"
else
    pip3 install jupyterlab
    echo "jupyterlab is new installed"
fi

