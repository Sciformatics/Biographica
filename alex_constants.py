import os
from sqlalchemy import create_engine 
# ENVIRONMENT VARIABLES
USER = os.environ['LOGNAME']

#DB INSTANTIATION
ENGINE = create_engine(f'sqlite:///{os.getcwd()}/data/test_database.db')

#FILE PATHS
LOG_FILE = f'{os.getcwd()}/admin/logs.txt'

#DIRECTORY PATHS
DOWNLOADS_DIR = f'{os.getcwd()}/downloads/'
TESTS_DIR = f'{os.getcwd()}/tests/'
DATA_DIR = f'{os.getcwd()}/data/'