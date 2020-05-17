#!/bin/bash
DATA_URL='https://data.lacity.org/api/views/yv23-pmwf/rows.csv?accessType=DOWNLOAD'

curl $DATA_URL > ./data/raw/permits_raw.csv