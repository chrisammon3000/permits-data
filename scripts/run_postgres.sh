#!/bin/bash
export CONTAINER=postgres_db

# check if data directory is present
[[ -d "./postgres/data" ]] || mkdir -p ./postgres/pgdata

[[ $(docker ps -f "name=$CONTAINER" --format '{{.Names}}') != "$CONTAINER" ]] || docker rm -f $CONTAINER

docker build -t $REPO:$CONTAINER ./postgres \
&& docker run --name $CONTAINER -d -p $DB_PORT:$DB_PORT \
-v "$PWD/postgres/pgdata":/var/lib/postgresql/data $REPO:$CONTAINER

#docker logs -f $CONTAINER