#!/bin/bash
CONTAINER=postgres_db

# check if data directory is present
[[ -d "./postgres/data" ]] || mkdir -p ./postgres/pgdata

[[ $(docker ps -f "name=$CONTAINER" --format '{{.Names}}') != "$CONTAINER" ]] || docker rm -f $CONTAINER

docker build -t permits-data:$CONTAINER ./postgres \
&& docker run --rm --name $CONTAINER -d -p 5432:5432 \
-v "$PWD/postgres/pgdata":/var/lib/postgresql/data permits-data:$CONTAINER

#docker logs -f $CONTAINER