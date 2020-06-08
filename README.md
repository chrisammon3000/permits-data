permits-data
==============================

ETL pipeline for construction permit data in Los Angeles county, USA.

## Pipeline Overview
The pipeline initializes a PostgreSQL database instance running inside a Docker container and loads raw construction permit data from a csv file in order to prepare it for analysis and modeling. Everything can be run with a single command `make data`. 

The pipeline performs the following steps in order to prepare the data for analysis:
1) Starts a PostgreSQL Docker container 
2) Loads the raw data from csv
3) Standardizes the column names
4) Updates the data types
5) Concatenates address fields into a single column `full_address`
6) Geocodes missing GPS coordinates using the `full_address`
7) Creates separate columns for `latitude` and `longitude`
8) Updates the database with the new values

## Getting Started

### Prerequisites
1) Install [Anaconda](https://docs.anaconda.com/anaconda/install/) package manager
2) Acquire an [API key for Google Maps](https://developers.google.com/maps/documentation/geocoding/get-api-key). It may be necessary to set up a developer account.
3) Check that the .env file is correct:
   ```
   # PostgreSQL
   REPO=permits-data
   CONTAINER=postgres_db
   POSTGRES_USER=postgres
   POSTGRES_PASSWORD=password
   POSTGRES_DB=permits
   DB_HOST=localhost
   DB_PORT=5432
   DATA_URL='https://data.lacity.org/api/views/yv23-pmwf/rows.csv?accessType=DOWNLOAD'
   PG_URI="postgres://${POSTGRES_USER}:${POSTGRES_PASSWORD}@${DB_HOST}:${DB_PORT}/shield"
   PGDATA="${PWD}/postgres/pgdata"
   DATA_DIR="${PWD}/data"

   # Google Maps
   GOOGLE_API_KEY="AIzaSyC4fTxcjqVAhrN_9ZenhkFIaJS15uctBMQ"
   GOOGLE_AGENT="permits-data"
   ```

### Setting up Environment

Clone the directory:
```
git clone <repo>
```
To create the environment run:
```
make create_env
conda activate permits-data-env
```
Populate the environment variables by running:
```
set -o allexport; source .env; set +o allexport;
```

### Running the Pipeline

To run the entire pipeline start to finish:
```
make data
```

To load the database and run the pipeline from Jupyter Notebook
```
make data \
&& cd notebooks \
&& jupyter notebook ## Select pipeline notebook
```

