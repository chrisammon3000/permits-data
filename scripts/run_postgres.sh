#!/bin/bash
export CONTAINER=postgres_db

# check if data directory is present
[[ -d "./postgres/pgdata" ]] || mkdir -p ./postgres/pgdata

# Check if container exists
if [[ $(docker ps -a -f "name=$CONTAINER" --format '{{.Names}}') != "$CONTAINER" ]]
then
    echo "Building new container..."
    docker build -q -t $REPO:$CONTAINER ./postgres \
    && docker run --name $CONTAINER -d -p $DB_PORT:$DB_PORT \
    -v "$PWD/postgres/pgdata":/var/lib/postgresql/data \
    -v "$PWD/data":/var/local/data $REPO:$CONTAINER
else
    echo "Starting container:"
    docker start $CONTAINER
    echo "### Container Ready. ###"
fi

#docker logs -f $CONTAINER