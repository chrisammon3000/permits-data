#!/bin/bash

echo "Starting database ..."
until pg_isready -q -h $DB_HOST -p $DB_PORT -U $POSTGRES_USER; do
    printf '.' ; \
    sleep 2 ; \
done
printf "%s\n" " "
echo "Connected at postgres://${POSTGRES_USER}:${POSTGRES_PASSWORD}@${DB_HOST}:${DB_PORT}"