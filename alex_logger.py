from datetime import datetime
from alex_constants import *
import os

def logger(message):
    with open(LOG_FILE, 'a') as file_in:
        datetime_now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        file_in.write(f'{USER} | {datetime_now} | {message}')
        file_in.write('\n')

