#!/usr/bin/env bash

# update
apt-get update
apt-get -y upgrade

# Install git, python, pip
apt-get -y install git python python-pip
apt-get autoremove

# install python deps
pip install --upgrade pip
pip install pyfscache sqlitedict unicodecsv lxml astroid tagme unicodecsv langdetect flask
# install scipy (-no-cache-dir because: -no-cache-dir)
pip install --no-cache-dir scipy

# Install Mongodb
# apt-get -y install mongodb-server
