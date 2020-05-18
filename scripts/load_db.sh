#!/bin/bash
TABLE=permits_raw
DATA='./data/raw/permits_raw.csv'

# Copy 1 million rows into db; For smaller dataset replace cat with head -n #rows
head -n 10001 $DATA | docker exec -i $CONTAINER psql -h $DB_HOST \
-U $POSTGRES_USER -p $DB_PORT $POSTGRES_DB -c "COPY $TABLE FROM STDIN WITH (FORMAT CSV, HEADER TRUE);" || \
echo "Database not created. Please run: make tear_down && make start_db"