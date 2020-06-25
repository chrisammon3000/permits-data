permits-data
==============================

A simple ETL pipeline for construction permits data from the [Los Angeles Open Data Portal](https://data.lacity.org/) using bash, Python, Docker and PostgreSQL. Run the command `make data` to automatically download contruction permits data, load into a PostgreSQL database in Docker, transform columns and geocode missing addresses. Includes a basic [Object-Relational Mapper](https://en.wikipedia.org/wiki/Object-relational_mapping) (ORM) for PostgreSQL using `psycopg2` and a notebook that outlines the steps in the pipeline.

## Background
Cited from [Building and Safety Permit Information](https://data.lacity.org/A-Prosperous-City/Building-and-Safety-Permit-Information-Old/yv23-pmwf):<br>
>*"The Department of Building and Safety issues permits for the construction, remodeling, and repair of buildings and structures in the City of Los Angeles. Permits are categorized into building permits, electrical permits, and mechanical permits"*

The raw permits data available from the [Los Angeles Open Data Portal](https://data.lacity.org/) contains missing latitude and longitude coordinates for some properties. The pipeline geocodes the missing coordinates and updates a local database.

### Data source
Data can be downloaded directly here:<br>
https://data.lacity.org/api/views/yv23-pmwf/rows.csv?accessType=DOWNLOAD

## Pipeline Overview
Data is downloaded to csv and loaded into a Docker PostgreSQL container. Columns are transformed and the database is updated. Everything can be run with a single command `make data` which will execute these steps:
1) Start a PostgreSQL Docker container 
2) Download raw data if not already present
3) Load a sample (1000 rows) of the raw data from csv
4) Standardize the column names
5) Update the data types
6) Concatenate address fields into a single column `full_address`
7) Geocode missing GPS coordinates using the `full_address`
8) Create separate columns for `latitude` and `longitude`
9) Update the database with the new values

## Built With
The pipeline is built on these frameworks and platforms:
* [psycopg2](https://pypi.org/project/psycopg2/)
* [pandas](https://pandas.pydata.org/)
* [Google Maps Platform](https://developers.google.com/maps/documentation) ([Geocoding API](https://developers.google.com/maps/documentation/geocoding/start))
* [Cookiecutter Data Science](https://drivendata.github.io/cookiecutter-data-science/)
* [PostgreSQL](https://www.postgresql.org/)
* [Docker](https://docs.docker.com/get-docker/)
* [GNU Make](https://www.gnu.org/software/make/)

In addition to the above packages, I built a simple Object-Relational Mapper (ORM) on top of psycopg2 to interface with PostgreSQL. The ORM package contains two classes, `Database` and `Table`, which contain the basic functionality
to run the pipeline. The package module is located in `src/toolkits/postgresql.py` and it's use is demonstrated in the notebook `0.1-pipeline.ipynb`.

## Getting Started

### Prerequisites
1) [Anaconda](https://docs.anaconda.com/anaconda/install/)
2) [Docker](https://docs.docker.com/get-docker/)
3) [API key for Google Maps](https://developers.google.com/maps/documentation/geocoding/get-api-key). It may be necessary to set up a developer account. Note that geocoding incurs a charge of $0.005 USD per request, although Google does give an intial $300 USD credit.

### Setting up Environment
Clone the directory:
  ```
  git clone <repo>
  ```
Check that an .env file exists with the following variables, if it does not simply copy and paste:
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
  conda activate permits_pipeline_env
  ```
Populate the environment variables by running:
  ```
  set -o allexport; source .env; set +o allexport;
  ```

### Running the Pipeline

  Option 1: Run the entire pipeline start to finish using GNU Make:
  ```
  make data
  ``` 

  Option 2: Load the raw data using GNU Make and run the rest of the pipeline from Jupyter Notebook
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
It is useful to check that new columns were correctly populated by running a query such as:
```
SELECT full_address, latitude, longitude FROM permits_raw LIMIT 10;
```

### Cleaning up
A single command will delete the database as well as the Docker container and any cache files:
```
make tear_down
```


## Contributors

**Primary (Contact) : [Gregory Lindsey](https://github.com/gclindsey)**