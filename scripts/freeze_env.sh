#!/bin/bash

# Creates or updates environment.yml and requirements.txt after installing new packages
echo "Saving environment..."
conda env export | grep -v "^prefix: " > environment.yml 
pip freeze > requirements.txt \
&& cat ./base-requirements.txt >> requirements.txt
echo "Saved."