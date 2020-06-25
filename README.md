permits-data
==============================

Simple ETL pipeline for construction permit data in Los Angeles county, USA using bash, Python, Docker and PostgreSQL. Run `make data` to automatically download contruction permits data, load into a PostgreSQL database in Docker, transform columns and geocode missing addresses. 

## Built With
The pipeline is built on these frameworks and platforms:
* [psycopg2](https://pypi.org/project/psycopg2/)
* [pandas](https://pandas.pydata.org/)
* [Google Maps Platform](https://developers.google.com/maps/documentation) ([Geocoding API](https://developers.google.com/maps/documentation/geocoding/start))
* [Cookiecutter Data Science](https://drivendata.github.io/cookiecutter-data-science/)
* [PostgreSQL](https://www.postgresql.org/)
* [Docker](https://docs.docker.com/get-docker/)
* [GNU Make](https://www.gnu.org/software/make/)

In addition to the above packages, I built a simple Object-Relational Mapper (ORM) on top of psycopg2 to interface with PostgreSQL. The ORM package contains two classes, Database and Table, which contain the basic functionality
to run the pipeline. The package module is located in `src/toolkits/postgresql.py`.

## Pipeline Overview
The permits-data pipeline initializes a PostgreSQL database instance running inside a Docker container and loads raw construction permit data from a csv file. It then extracts, transforms and reloads the data to make it ready for analysis. 

Everything can be run with a single command `make data` which will execute these steps:
1) Start a PostgreSQL Docker container 
2) Load the raw data from csv
3) Standardize the column names
4) Update the data types
5) Concatenate address fields into a single column `full_address`
6) Geocode missing GPS coordinates using the `full_address`
7) Create separate columns for `latitude` and `longitude`
8) Update the database with the new values

## Getting Started

### Prerequisites
1) Install [Anaconda](https://docs.anaconda.com/anaconda/install/) package manager
2) Install [Docker](https://docs.docker.com/get-docker/)
3) Acquire an [API key for Google Maps](https://developers.google.com/maps/documentation/geocoding/get-api-key). It may be necessary to set up a developer account. Note that geocoding incurs a charge of $0.005 USD per request, although Google does give an intial $300 USD credit.

### Setting up Environment
Clone the directory:
  ```
  git clone <repo>
  ```
Check that an .env file exists with the following variables:
   ```
   ### .env

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

   # Google Maps API
   GOOGLE_API_KEY="<your api key>" # From the Geocoding API
   GOOGLE_AGENT="permits-data" # or the GCP Project ID used when creating the API key
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

  Or load the raw data and run the pipeline from Jupyter Notebook
  ```
  make load_db \
  && cd notebooks \
  && jupyter notebook ## Select 0.1-pipeline notebook
  ```

### Accessing the database
The PostgreSQL database within the Docker container can be accessed by running:
```
docker exec -it postgres_db psql -U postgres -d permits
```
This is useful to check that new columns were correctly populated.