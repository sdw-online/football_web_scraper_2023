import io
import os
import boto3
from typing import List, Optional, Any
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


def create_logger(name, level=logging.DEBUG):
    logger = logging.getLogger(name)
    logger.setLevel(level)
    return logger


def create_file_handler(local_filepath, level=logging.DEBUG, log_format='%(asctime)s | %(levelname)s | %(message)s'):
    file_handler = logging.FileHandler('logs/scraper/' + local_filepath + '.log', mode='w')
    file_handler.setLevel(level)
    formatter = logging.Formatter(log_format)
    file_handler.setFormatter(formatter)
    return file_handler


def create_console_handler(colored=True, level=logging.DEBUG, detailed_logs=False):
    console_handler = logging.StreamHandler()
    
    if colored:
        console_formatter = coloredlogs.ColoredFormatter(fmt='%(message)s', level_styles=dict(
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
        coloredlogs.install(level=level)
    else:
        if detailed_logs:
            detailed_log_format = '%(asctime)s | %(levelname)s | %(message)s'
            console_formatter = logging.Formatter(detailed_log_format)
        else:
            simple_log_format = '%(message)s'
            console_formatter = logging.Formatter(simple_log_format)
    
    console_handler.setFormatter(console_formatter)
    return console_handler


def log_event(logger, message, **kwargs):
    level = kwargs.get('level', logging.INFO)
    logger.log(level, message)






# ================================================ CONFIG ================================================

def create_config(aws_access_key: str, aws_secret_key: str, aws_region_name: str, aws_s3_bucket: str, aws_s3_folder: str, local_target_path: str, WRITE_FILES_TO_CLOUD: bool = False, s3_client: Optional[Any]=None) -> dict:
    if s3_client is None:
        s3_client = boto3.client(
            "s3",
            aws_access_key_id       =   aws_access_key,
            aws_secret_access_key   =   aws_secret_key,
            region_name             =   aws_region_name
        )
    
    config = {
        "AWS_ACCESS_KEY":           aws_access_key,
        "AWS_SECRET_KEY":           aws_secret_key,
        "S3_REGION":                aws_region_name,
        "S3_BUCKET":                aws_s3_bucket,
        "S3_FOLDER":                aws_s3_folder,
        "LOCAL_TARGET_PATH":        local_target_path,
        "S3_CLIENT":                s3_client,
        "WRITE_FILES_TO_CLOUD":     WRITE_FILES_TO_CLOUD,
    }
    
    return config
    
    




# Initialize the functions 

def main():

    # ================================================ LOGGER ================================================
    logger_name         =   __name__
    local_filepath      =   'main_scraper'
    log_level           =   logging.DEBUG

    logger              =   create_logger(logger_name, log_level)
    file_handler        =   create_file_handler(local_filepath, log_level)
    console_handler     =   create_console_handler(colored=False, level=log_level, detailed_logs=False)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    log_event_as_debug      =   partial(log_event, logger, level=logging.DEBUG)
    log_event_as_info       =   partial(log_event, logger, level=logging.INFO)
    log_event_as_warning    =   partial(log_event, logger, level=logging.WARNING)
    log_event_as_critical   =   partial(log_event, logger, level=logging.CRITICAL)
    log_event_as_error      =   partial(log_event, logger, level=logging.ERROR)

    log_event_as_debug("This is debug message")
    log_event_as_info("This is info message")
    log_event_as_warning("This is warning message")
    log_event_as_critical("This is critical message")
    log_event_as_error("This is error message")



# ================================================ CONFIG ================================================

    
    aws_access_key          =   os.getenv("AWS_ACCESS_KEY")
    aws_secret_key          =   os.getenv("AWS_SECRET_KEY")
    aws_region_name         =   os.getenv("S3_REGION") 
    aws_s3_bucket           =   os.getenv("S3_BUCKET") 
    aws_s3_folder           =   os.getenv("S3_FOLDER") 
    local_target_path       =   os.getenv("LOCAL_TARGET_PATH") 

    config = create_config(aws_access_key, aws_secret_key, aws_region_name, aws_s3_bucket, aws_s3_folder, local_target_path, WRITE_FILES_TO_CLOUD=False)








# Instantiate the classes in this script

if __name__=="__main__":
    # ---------------------------------------- TEST ----------------------------------------  
    # Test the logging operation works as expected

    main()