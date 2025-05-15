#!/bin/bash

if [ ! -f .env ]; then
  touch .env
  echo ".env file created."
fi

prompt_input() {
  echo -ne "$1"
  read "$2"
}

prompt_input "Enter pgadmin email: " PGADMIN_DEFAULT_EMAIL

echo -ne "Enter pgadmin password: "
read -s PGADMIN_DEFAULT_PASSWORD && echo

prompt_input "Enter pgadmin server name: " PGADMIN_SERVER_NAME

prompt_input "Enter postgres user (optional): " POSTGRES_USER
POSTGRES_USER=${POSTGRES_USER:-airflow}

echo -ne "Enter postgres password: "
read -s POSTGRES_PASSWORD && echo
POSTGRES_PASSWORD=${POSTGRES_PASSWORD:-airflow}

prompt_input "Enter postgres db (optional): " POSTGRES_DB
POSTGRES_DB=${POSTGRES_DB:-postgres}

prompt_input "Enter airflow db (optional): " AIRFLOW_DB
AIRFLOW_DB=${AIRFLOW_DB:-airflow}

prompt_input "Enter posgres port (optional): " POSTGRES_PORT
POSTGRES_PORT=${POSTGRES_PORT:-5432}

echo "PGADMIN_DEFAULT_EMAIL=$PGADMIN_DEFAULT_EMAIL" > .env
echo "PGADMIN_DEFAULT_PASSWORD=$PGADMIN_DEFAULT_PASSWORD" >> .env
echo "PGADMIN_SERVER_NAME=$PGADMIN_SERVER_NAME" >> .env
echo "POSTGRES_USER=$POSTGRES_USER" >> .env
echo "POSTGRES_PASSWORD=$POSTGRES_PASSWORD" >> .env
echo "POSTGRES_DB=$AIRFLOW_DB" >> .env
echo "POSTGRES_PORT=$POSTGRES_PORT" >> .env

echo "AIRFLOW_CONN_BANNED_BOOKS=postgresql://$POSTGRES_USER:$POSTGRES_PASSWORD@postgres:$POSTGRES_PORT/$POSTGRES_DB" >> .env
echo "AIRFLOW_UID=50000" >> .env

echo "Environment variables set!"

set -e

python3 ./config/generate_pgadmin_server.py

echo "pgAdmin server configuration completed!"

docker-compose up -d
