#!/bin/bash
TABLE=permits_raw
DATA="./data/raw/$RAW_DATA"
CONTAINER=postgres_db
export NROWS=1001

echo "Copying $(expr $NROWS - 1) rows into table..."
# Copy 1 million rows into db; For smaller dataset replace cat with head -n #rows
head -n $NROWS $DATA | docker exec -i $CONTAINER psql -h $DB_HOST \
-U $POSTGRES_USER -p $DB_PORT $POSTGRES_DB -c "COPY $TABLE FROM STDIN WITH (FORMAT CSV, HEADER TRUE);" || \
echo "Database not initialized correctly. Please run: make tear_down && make start_db"