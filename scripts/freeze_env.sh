#!/bin/bash

echo "Saving environment..."
conda env export > environment.yml \
&& cat ./environment.yml | sed "s:/Users/gregory:$HOME:g" > environment_tmp.yml \
&& mv environment_tmp.yml environment.yml
pip freeze > requirements.txt \
&& cat ./base-requirements.txt >> requirements.txt
echo "Done."