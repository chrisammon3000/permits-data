#!/bin/bash

echo "Saving environment..."
conda env export | grep -v "^prefix: " > environment.yml 
pip freeze > requirements.txt \
&& cat ./base-requirements.txt >> requirements.txt
echo "Done."