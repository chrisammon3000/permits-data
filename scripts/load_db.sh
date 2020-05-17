#!/bin/bash

DATA='./data/raw/permits_raw.csv'

# Copy 1 million rows into db; For smaller dataset replace cat with head -n #rows
head -n 10001 $DATA | docker exec -i postgres_db psql -h localhost \
-U postgres -p 5432 permits -c 'COPY permits_raw FROM STDIN WITH (FORMAT CSV, HEADER TRUE);'