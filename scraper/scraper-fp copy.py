import io
import os
import boto3
from typing import List
import pandas as pd
from time import sleep
from pathlib import Path
import logging, coloredlogs
from functools import partial
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support import expected_conditions as EC
from abc import ABC, abstractmethod



# ================================================ LOGGER ================================================

# Set up functions for logging events 


def create_file_handler(local_filepath: str=Path(__file__).stem, level=logging.DEBUG):
    file_handler = logging.FileHandler('logs/scraper' + local_filepath + '.log', mode='w')
    file_handler.setLevel(level)
    return file_handler


def set_file_formatter(file_handler, log_format: str='%(asctime)s | %(levelname)s | %(message)s' ):
    file_formatter = logging.Formatter(log_format)
    file_handler.setFormatter(file_formatter)




def create_console_handler(level=logging.DEBUG):
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    return console_handler


def set_coloured_formatter(console_handler):
    console_formatter = coloredlogs.ColoredFormatter(
        fmt='%(message)s',
        level_styles=dict(
            debug=dict(color='white'),
            info=dict(color='green'),
            warning=dict(color='cyan'),
            error=dict(color='red', bold=True, bright=True),
            critical=dict(color='black', bold=True, background='red')
        ),
        field_styles=dict(
            messages=dict(color='white')
        )
    )
    console_handler.setFormatter(console_formatter)
    coloredlogs.install(level=console_handler.level)

    return console_handler


def set_console_formatter(console_handler, detailed_logs=False):
    if detailed_logs:
        console_formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s')
    else:
        console_formatter = logging.Formatter('%(message)s')
    console_handler.setFormatter(console_formatter)


def create_logger(level=logging.DEBUG, coloured=True, detailed_logs=False):
    logger = logging.getLogger(__name__)
    logger.setLevel(level)
    console_handler = create_console_handler(level)

    if coloured:
        set_coloured_formatter(console_handler)
    else:
        set_console_formatter(console_handler, detailed_logs)

    logger.addHandler(console_handler)
    
    return logger


# def log_event(logger, level, message):
#     logger.log(level, message)


def log_event(logger, message, **kwargs):
    level = kwargs.get('level', logging.INFO)






log_event_as_debug      =   partial(log_event, level=logging.DEBUG)
log_event_as_info       =   partial(log_event, level=logging.INFO)
log_event_as_warning    =   partial(log_event, level=logging.WARNING)
log_event_as_critical   =   partial(log_event, level=logging.CRITICAL)
log_event_as_error      =   partial(log_event, level=logging.ERROR)
    







# # ================================================ CONFIG ================================================




def main():
    # # Specify the constants for the scraper
    # local_target_path               =   os.path.abspath('temp_storage/dirty_data')
    # match_date                      =   '2023-Apr-24'
    # football_url                    =   f'https://www.twtd.co.uk/league-tables/competition:premier-league/daterange/fromdate:2022-Jul-01/todate:{match_date}/type:home-and-away/'


    logger_name = __name__
    logger_level = logging.DEBUG
    


    logger = create_logger(level=logger_level, coloured=False, detailed_logs=False)


    print_debug = partial(log_event_as_debug, logger)

    print_debug("This is a test message!!!!")






# Instantiate the classes in this script

if __name__=="__main__":

    # ---------------------------------------- TEST ----------------------------------------  

    # Test the logging operation works as expected

    
    main()