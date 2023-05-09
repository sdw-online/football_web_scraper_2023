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
from selenium.webdriver.remote.webelement import WebElement
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


def log_event(logger, level, message):
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
    
    



# ================================================ WEBPAGE LOADER ================================================

def create_webdriver(options=None, service=None) -> webdriver.Chrome:
    if options is None:
        options = webdriver.ChromeOptions()
    if service is None:
        service = Service(executable_path=ChromeDriverManager().install())
    
    return webdriver.Chrome(options=options, service=service)
    

def load_webpage(chrome_driver: webdriver.Chrome, url: str, logger: logging.Logger) -> webdriver.Chrome:
    log_event(logger, logging.DEBUG, ">>> Loading webpage using Selenium ...")
    chrome_driver.get(url)
    sleep(3)
    return chrome_driver


def check_page_title(chrome_driver: webdriver.Chrome, expected_title: str, logger: logging.Logger) -> None:
    assert expected_title in chrome_driver.title, f"ERROR: Unable to load site for {expected_title} ... "
    log_event(logger, logging.DEBUG, ">>> Webpage successfully loaded ...")


def load_prem_league_table_webpage(url: str, logger: logging.Logger, options=None, service=None) -> webdriver.Chrome:
    chrome_driver = create_webdriver(options, service)
    chrome_driver = load_webpage(chrome_driver, url, logger)
    check_page_title(chrome_driver, "Premier League", logger)
    
    return chrome_driver




# ================================================ POPUP HANDLER ================================================

def find_popup_element(chrome_driver: webdriver.Chrome, timeout: int) -> WebElement:
    wait = WebDriverWait(chrome_driver, timeout)

    return wait.until(EC.presence_of_element_located((By.XPATH, '/html/body/div[8]/div[2]/div[1]/div[1]/button/i')))


def close_popup(close_popup_box: WebElement, logger: logging.Logger) -> None:
    close_popup_box.click()
    log_event(logger, logging.DEBUG, f'>>>>   Closing cookie pop-up window ...')


def close_popup_box_for_prem_league_table_webpage(chrome_driver: webdriver.Chrome, logger: logging.Logger, timeout: int = 5) -> None:
    try:
        close_popup_box = find_popup_element(chrome_driver, timeout)
        close_popup(close_popup_box, logger)
    except Exception as e:
        log_event(logger, logging.ERROR, f'No cookie pop-up window to close...let\'s begin scraping for league standings !!') 





# ================================================ DATA EXTRACTOR ================================================


def scrape_table_standings(chrome_driver: webdriver.Chrome, logger: logging.Logger) -> Optional[Any]:
    try:
        log_event(logger, logging.DEBUG, f'>>>>   Detecting HTML elements ...')
        return chrome_driver.find_element(By.CLASS_NAME, 'leaguetable')
    except Exception as e:
        log_event(logger, logging.ERROR, e)
        return None


def scrape_table_rows(table, logger: logging.Logger) -> Optional[Any]:
    log_event(logger, logging.DEBUG, f'>>>>   Extracting content from HTML elements ...')
    return table.find_elements(By.XPATH, './/tr') if table else []


def scrape_data_from_cells(table_row, logger: logging.Logger, row_counter: int):
    cells       =   table_row.find_elements(By.TAG_NAME, 'td')
    cell_data   =   [cell.text for cell in cells]
    for i, data in enumerate(cell_data):
        log_event(logger, logging.DEBUG, f'>>>>   Table row no "{row_counter}", Cell no "{i+1}" appended ...')
    return cell_data


def scrape_data_from_rows(table_rows, logger: logging.Logger):
    return [scrape_data_from_cells(table_row, logger, i+1) for i, table_row in enumerate(table_rows) ]


def extract_data(chrome_driver: webdriver.Chrome, logger: logging.Logger):
    prem_league_table   =   scrape_table_standings(chrome_driver, logger)
    table_rows          =   scrape_table_rows(prem_league_table, logger)
    return scrape_data_from_rows(table_rows, logger)




# ================================================ DATA TRANSFORMER ================================================


def create_dataframe(scraped_data, scraped_columns, match_date, logger: logging.Logger):
    try:
        log_event(logger, logging.DEBUG, '>>>> Now creating dataframe for Premier League table standings ....')
        table_df = pd.DataFrame(data=scraped_data, columns=scraped_columns)
        table_df['match_date'] = match_date
        log_event(logger, logging.DEBUG, '>>>> Dataframe for Premier League table successfully created....')
    
    except Exception as e:
        log_event(logger, logging.ERROR, e)
    
    return table_df


def transform_data(scraped_content: List[List[str]], match_date: str, logger: logging.Logger) -> pd.DataFrame:
    try:
        log_event(logger, logging.DEBUG, '>>>> Now transforming scraped content for Premier League table standings ....')
        scraped_data = scraped_content[1:]
        scraped_columns = scraped_content[0]
        log_event(logger, logging.DEBUG, '>>>> Successfully scraped content for Premier League table standings ....')
       
    except Exception as e:
        log_event(logger, logging.ERROR, e)

    return create_dataframe(scraped_data, scraped_columns, match_date, logger)


    
    

# ================================================ DATA UPLOADER ================================================



# A. UPLOAD TO CLOUD

def create_s3_key(s3_folder, file_name, match_date, logger: logging.Logger):
    try:
        log_event(logger, logging.DEBUG, '>>>> Creating S3 key for Prem League table file ...')
        return f"{s3_folder}/{file_name}_{match_date}.csv"
    except Exception as e:
        log_event(logger, logging.ERROR, e)



def create_csv_buffer(logger: logging.Logger):
    try:
        log_event(logger, logging.DEBUG, f">>> Creating in-memory buffer ...")
        return io.StringIO()
    
    except Exception as e:
        log_event(logger, logging.ERROR, e)



def write_df_to_csv(df, csv_buffer, logger: logging.Logger):
    try:
        log_event(logger, logging.DEBUG, f">>> Persisting dataframe to CSV file ...")
        print(f'Dataframe: {df} ')
        df.to_csv(csv_buffer, index=False)
        print(f'CSV BUFFER (1): {csv_buffer} ')

    except Exception as e:
        log_event(logger, logging.ERROR, e)



def get_string_values_from_buffer(csv_buffer, logger: logging.Logger):
    try:
        log_event(logger, logging.DEBUG, f">>> Retrieving data from CSV buffer & storing as string values ...")
        print(f'CSV BUFFER (2): {csv_buffer} ')
        print(f'OTHER: {csv_buffer.getvalue()}   ')
        return csv_buffer.getvalue()
    
    except Exception as e:
        log_event(logger, logging.ERROR, e)


def upload_string_to_s3(s3_client, s3_bucket, s3_key, string_values, logger: logging.Logger):
    try:
        log_event(logger, logging.DEBUG, f">>> Preparing to upload file to S3 bucket ...")
        s3_client.put_object(Bucket=s3_bucket, Key=s3_key, Body=string_values)
        
    except Exception as e:
        log_event(logger, logging.ERROR, e)



def upload_df_to_s3(df, match_date, file_name, config, logger):
    try:
        log_event(logger, logging.DEBUG, f">>> Composing final operations to begin upload to cloud ...")
        s3_key                              =   create_s3_key(config["S3_FOLDER"], file_name, match_date, logger)
        CSV_BUFFER                          =   create_csv_buffer(logger)
        write_df_to_csv(df, CSV_BUFFER, logger)
        RAW_TABLE_ROWS_AS_STRING_VALUES     =   get_string_values_from_buffer(CSV_BUFFER, logger)
        print(f'STRING VALUES: {RAW_TABLE_ROWS_AS_STRING_VALUES} ')
        
        try:
            upload_string_to_s3(s3_client=config["S3_CLIENT"], s3_bucket=config["S3_BUCKET"], s3_key=s3_key, string_values=RAW_TABLE_ROWS_AS_STRING_VALUES, logger=logger)
            log_event(logger, logging.DEBUG, f">>> Successfully written and loaded '{file_name}' file to cloud target location in S3 bucket... ")
        except Exception as e:
            log_event(logger, logging.ERROR, e)

    except Exception as e:
        log_event(logger, logging.ERROR, e)
    



# B. UPLOAD TO LOCAL MACHINE

def create_local_file_path(target_path, file_name, match_date, logger: logging.Logger):
    try:
        log_event(logger, logging.DEBUG, '>>>> Creating Prem League table filepath  ...')
        return f"{target_path}/{file_name}_{match_date}.csv"

    except Exception as e:
        log_event(logger, logging.ERROR, e)


def write_df_to_local_file(df, file_path, logger: logging.Logger):
    try:
        log_event(logger, logging.DEBUG, f">>> Converting data frame to CSV ...")
        df.to_csv(file_path, index=False)

    except Exception as e:
        log_event(logger, logging.ERROR, e)



def upload_df_to_local_file(df, match_date, file_name, config, logger: logging.Logger):
    try:
        log_event(logger, logging.DEBUG, f">>> Composing final operations to begin upload to local machine ...")
        file_path = create_local_file_path(config["LOCAL_TARGET_PATH"], file_name, match_date, logger)

        try:
            write_df_to_local_file(df, file_path, logger)
            log_event(logger, logging.DEBUG, f">>> Successfully written and loaded '{file_name}' file to local target location... ")
        except Exception as e:
            log_event(logger, logging.error, e)

    except Exception as e:
        log_event(logger, logging.error, e)














# Initialize the functions 

def main():

    match_date                      =   '2023-May-09'
    football_url                    =   f'https://www.twtd.co.uk/league-tables/competition:premier-league/daterange/fromdate:2022-Jul-01/todate:{match_date}/type:home-and-away/'



    # Set up logging to file and console
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

    # log_event_as_debug("This is debug message")
    # log_event_as_info("This is info message")
    # log_event_as_warning("This is warning message")
    # log_event_as_critical("This is critical message")
    # log_event_as_error("This is error message")




    # Set up credentials for configuration 

    aws_access_key          =   os.getenv("AWS_ACCESS_KEY")
    aws_secret_key          =   os.getenv("AWS_SECRET_KEY")
    aws_region_name         =   os.getenv("S3_REGION") 
    aws_s3_bucket           =   os.getenv("S3_BUCKET") 
    aws_s3_folder           =   os.getenv("S3_FOLDER") 
    local_target_path       =   os.getenv("LOCAL_TARGET_PATH") 
    config                  =   create_config(aws_access_key, aws_secret_key, aws_region_name, aws_s3_bucket, aws_s3_folder, local_target_path, WRITE_FILES_TO_CLOUD=False)




    # Set up Selenium Chrome driver and configuration settings  
    chrome_driver = load_prem_league_table_webpage(football_url, logger)



    # Close popup box on webpage
    close_popup_box_for_prem_league_table_webpage(chrome_driver, logger)


    # Extract data (E)
    scraped_content          =   extract_data(chrome_driver, logger)
    

    # Transform data (T)
    prem_league_df           =   transform_data(scraped_content, match_date, logger)


    # Load data (L)
    file_name = "prem_league_table"
    config["WRITE_FILES_TO_CLOUD"] = True

    if config["WRITE_FILES_TO_CLOUD"]:
        upload_df_to_s3(prem_league_df, match_date, file_name, config, logger)
    else:
        upload_df_to_local_file(prem_league_df, match_date, file_name, config, logger)



    # Close driver when scraping is completed 
    chrome_driver.quit()



# Instantiate the classes in this script

if __name__=="__main__":
    # ---------------------------------------- TEST ----------------------------------------  
    # Test the logging operation works as expected

    main()