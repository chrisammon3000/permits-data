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
In order to run the pipeline Anaconda will need to be installed and a Google Maps Geocode API key should be present in the .env file.

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
To start and load the database, from the root directory run:
```
make data
```
