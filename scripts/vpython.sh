#!/bin/bash

vpython27()
mkdir -p tmp/test1/test2
{
 sudo mkdir -p /data/0
 sudo chown -R ${USER}:${USER} /data/0
 sudo pip install virtualenv

 export VIRTUAL_PYTHON_DIR=/data/0/vpython
 virtualenv $VIRTUAL_PYTHON_DIR
 ln -fs  $VIRTUAL_PYTHON_DIR $HOME/vpython
}

python35_venv()
{
 sudo apt-get install python3 python3-pip build-essential
 pip3 install virtualenv

 sudo mkdir -p /data/0
 sudo chown -R ${USER}:${USER} /data/0
 virtualenv -p `which python3` /data/0/vpython35
 ln -fs /data/0/vpython35 $HOME/vpython35
}


python35_venv
