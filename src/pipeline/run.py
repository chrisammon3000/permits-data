# -*- coding: utf-8 -*-
import os
import sys
from pathlib import Path
sys.path[0] = str(Path(__file__).resolve().parents[2]) # Set path for modules
import pandas as pd
import psycopg2

if __name__ == '__main__':

    # Get project root directory
    project_dir = str(Path(__file__).resolve().parents[2])

