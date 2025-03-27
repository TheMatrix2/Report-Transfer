#!/bin/bash

sudo apt-get update

sudo apt-get install -y python3 python3-pip python3-venv

sudo apt-get install -y mariadb-server

sudo mysql_secure_installation

python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

sudo mysql -u root -p < db_user_setup.sql

echo "Dependencies installed successfully!"