permits-data
==============================

Analysis of construction permits in Los Angeles, USA.

## Getting Started
To fire up the db, from the root directory run:
```
set -o allexport; source .env; set +o allexport; \
make data
```
## File Structure
The `data` folder is organized as such:

    ├── data
    │   ├── external       <- Data from third party sources such as GIS.
    │   ├── interim        <- Transformed data passed to `make update_db`
    │   ├── processed      <- The final, canonical data sets for modeling.
    │   └── raw            <- The original, immutable data dump. 