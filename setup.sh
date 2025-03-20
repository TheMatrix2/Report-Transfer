#!/bin/bash

sudo apt-get update

sudo apt-get install -y python3 python3-pip python3-venv

sudo apt-get install -y mariadb-server

sudo mysql_secure_installation

python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

sudo mysql -e "CREATE USER IF NOT EXISTS 'metrika_user'@'localhost' IDENTIFIED BY 'your_secure_password';"
sudo mysql -e "GRANT ALL PRIVILEGES ON yandex_metrika.* TO 'metrika_user'@'localhost';"
sudo mysql -e "FLUSH PRIVILEGES;"

echo "Dependencies installed successfully!"