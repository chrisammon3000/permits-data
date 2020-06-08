.PHONY: clean data lint requirements sync_data_to_s3 sync_data_from_s3

#################################################################################
# GLOBALS                                                                       #
#################################################################################

PROJECT_DIR := $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))
BUCKET = [OPTIONAL] your-bucket-for-syncing-data (do not include 's3://')
PROFILE = default
PROJECT_NAME = permits-data
PYTHON_INTERPRETER = python3
SHELL=/bin/bash
CONDAROOT=/Users/gregory/anaconda3
export CONDA_ENV=permits-data-env

ifeq (,$(shell which conda))
HAS_CONDA=False
else
HAS_CONDA=True
endif

#################################################################################
# COMMANDS                                                                      #
#################################################################################

## Delete Conda environment
delete_env:
	@source $(CONDAROOT)/bin/activate
	@conda env remove --name $(CONDA_ENV)

## Create Conda environment
create_env: delete_env
	@echo "###Create environment###"
	@source $(CONDAROOT)/bin/activate \
	&& conda env create -f environment.yml \
	&& conda deactivate

## Check environment variables
check_env:
	@if [[ "$$CONDA_DEFAULT_ENV" != "$$CONDA_ENV" ]]; then \
		echo "Error: Environment not active. To activate run:" \
		&& echo "conda activate $(CONDA_ENV)"; else echo "Conda environment ready."; fi
	@if [ -z "$$CONTAINER" ]; then echo "Error: Missing environment variables. To set them first run:" \
		&& echo "set -o allexport; source .env; set +o allexport;"; else echo "Environment variables ready."; fi

## Create data directory if not present
check_directory: 
	@if [ ! -d "./data" ]; then mkdir -p data/{interim,processed,raw}; fi

fetch_data: check_directory
	@if [ ! -f "$$PWD/data/raw/permits_raw.csv" ]; then echo "Downloading data..." \
		&& curl $$DATA_URL > $(PWD)/data/raw/permits_raw.csv; fi
	@echo "Data is ready."

## Start Postgres
start_db: check_env check_directory
	@echo "### Starting Docker... ###"
	@scripts/run_postgres.sh
	@echo "### Waiting for PostgreSQL... ###"
	@scripts/test_connection.sh

## Load data
load_db: start_db
	@echo "### Loading PostgreSQL Database... ###"
	@scripts/load_db.sh
	@echo "Database is loaded."
	
## Load cleaned data
data: load_db
	@echo "### Updating PostgreSQL Database... ###"
	@$(PYTHON_INTERPRETER) src/pipeline/run.py
	@echo "### End Pipeline ###"

## Stops database
stop_db:
	@echo "### Stopping PostgreSQL Database... ###"
	@echo "Container stopped:"
	@docker stop $(CONTAINER) ||:

## Delete ./postgres/pgdata folder and contents
clear_db: stop_db
	@echo "### Deleting PostgreSQL Database... ###"
	@echo 'Removing files in ./postgres/pgdata/ ...'
	@echo "Enter password to continue:"
	@sudo rm -rf ./postgres/pgdata/
	@echo "Database deleted."

## Remove db container
clear_docker: clear_db
	@echo "### Removing Container... ###"
	@echo "Container removed:"
	@docker rm $(CONTAINER)
	@echo "Done."

## Removes deletes db and cleans up project files, keeps downloaded data
tear_down: check_env clear_docker clean
	@echo "Tear down complete."

## Install Python Dependencies
requirements: test_environment
	$(PYTHON_INTERPRETER) -m pip install -U pip setuptools wheel
	$(PYTHON_INTERPRETER) -m pip install -r requirements.txt

## Delete all compiled Python files
clean:
	@echo "### Cleaning up... ###"
	@find . -type f -name "*.py[co]" -delete
	@find . -type d -name "__pycache__" -delete
	@echo "Cache files deleted."

## Lint using flake8
lint:
	flake8 src

## Upload Data to S3
sync_data_to_s3:
ifeq (default,$(PROFILE))
	aws s3 sync data/ s3://$(BUCKET)/data/
else
	aws s3 sync data/ s3://$(BUCKET)/data/ --profile $(PROFILE)
endif

## Download Data from S3
sync_data_from_s3:
ifeq (default,$(PROFILE))
	aws s3 sync s3://$(BUCKET)/data/ data/
else
	aws s3 sync s3://$(BUCKET)/data/ data/ --profile $(PROFILE)
endif

## Set up python interpreter environment
create_environment:
ifeq (True,$(HAS_CONDA))
		@echo ">>> Detected conda, creating conda environment."
ifeq (3,$(findstring 3,$(PYTHON_INTERPRETER)))
	conda create --name $(PROJECT_NAME) python=3
else
	conda create --name $(PROJECT_NAME) python=2.7
endif
		@echo ">>> New conda env created. Activate with:\nsource activate $(PROJECT_NAME)"
else
	$(PYTHON_INTERPRETER) -m pip install -q virtualenv virtualenvwrapper
	@echo ">>> Installing virtualenvwrapper if not already installed.\nMake sure the following lines are in shell startup file\n\
	export WORKON_HOME=$$HOME/.virtualenvs\nexport PROJECT_HOME=$$HOME/Devel\nsource /usr/local/bin/virtualenvwrapper.sh\n"
	@bash -c "source `which virtualenvwrapper.sh`;mkvirtualenv $(PROJECT_NAME) --python=$(PYTHON_INTERPRETER)"
	@echo ">>> New virtualenv created. Activate with:\nworkon $(PROJECT_NAME)"
endif

## Test python environment is setup correctly
test_environment:
	$(PYTHON_INTERPRETER) test_environment.py

#################################################################################
# Self Documenting Commands                                                     #
#################################################################################

.DEFAULT_GOAL := help

# Inspired by <http://marmelab.com/blog/2016/02/29/auto-documented-makefile.html>
# sed script explained:
# /^##/:
# 	* save line in hold space
# 	* purge line
# 	* Loop:
# 		* append newline + line to hold space
# 		* go to next line
# 		* if line starts with doc comment, strip comment character off and loop
# 	* remove target prerequisites
# 	* append hold space (+ newline) to line
# 	* replace newline plus comments by `---`
# 	* print line
# Separate expressions are necessary because labels cannot be delimited by
# semicolon; see <http://stackoverflow.com/a/11799865/1968>
.PHONY: help
help:
	@echo "$$(tput bold)Available rules:$$(tput sgr0)"
	@echo
	@sed -n -e "/^## / { \
		h; \
		s/.*//; \
		:doc" \
		-e "H; \
		n; \
		s/^## //; \
		t doc" \
		-e "s/:.*//; \
		G; \
		s/\\n## /---/; \
		s/\\n/ /g; \
		p; \
	}" ${MAKEFILE_LIST} \
	| LC_ALL='C' sort --ignore-case \
	| awk -F '---' \
		-v ncol=$$(tput cols) \
		-v indent=19 \
		-v col_on="$$(tput setaf 6)" \
		-v col_off="$$(tput sgr0)" \
	'{ \
		printf "%s%*s%s ", col_on, -indent, $$1, col_off; \
		n = split($$2, words, " "); \
		line_length = ncol - indent; \
		for (i = 1; i <= n; i++) { \
			line_length -= length(words[i]) + 1; \
			if (line_length <= 0) { \
				line_length = ncol - indent - length(words[i]) - 1; \
				printf "\n%*s ", -indent, " "; \
			} \
			printf "%s ", words[i]; \
		} \
		printf "\n"; \
	}' \
	| more $(shell test $(shell uname) = Darwin && echo '--no-init --raw-control-chars')
