permits-data
==============================

Analysis of construction permits in Los Angeles, USA.

## Pipeline Overview
The pipeline initializes a PostgreSQL database instance running inside a Docker container and loads raw construction permits data from a csv file and prepare it for ETL, analysis and modeling. Everything can be run with a single command `make data`. The steps are broken down as follows:

1) Fetch data: `make fetch`
2) Start container: `make start_db`
3) Load raw data into database using COPY and update column names: `make load_db`
4) Extract the raw data and transform columns: `make transform_data`
5) Update the database with the transformed columns: `make update_db`

## Getting Started
Clone the directory:
```
git clone <repo>
```
It is recommended to first install Anaconda. To create the environment run:
```
make create_env
conda activate permits-data-env
```
Populate the environment variables by running:
```
set -o allexport; source .env; set +o allexport;
```
To fire up and load the db, from the root directory run:
```
make data
```
## File Structure
The `data` folder is organized as such:

    ├── data
    │   ├── external       <- Data from third party sources such as GIS.
    │   ├── interim        <- Transformed data passed to `make update_db`
    │   ├── processed      <- The final, canonical data sets for modeling.
    │   └── raw            <- The original, immutable data dump. 