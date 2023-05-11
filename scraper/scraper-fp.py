import io
import os
import boto3
import pandas as pd
from time import sleep
from pathlib import Path
import logging, coloredlogs
from datetime import datetime
from functools import partial
from dotenv import load_dotenv
from selenium import webdriver
from typing import List, Optional, Any, Dict
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support import expected_conditions as EC



# ================================================ LOGGER ================================================

# Set up functions for logging events 


def create_logger(name: str, 
                  log_level: int
                  ) -> logging.Logger:
    
    logger = logging.getLogger(name)
    logger.setLevel(log_level)
    return logger



def create_file_handler(local_filepath: str, 
                        log_level: int, 
                        log_format: str,
                        log_folder:str
                        ) -> logging.FileHandler:
    
    file_handler = logging.FileHandler(f'{log_folder}/{local_filepath}.log', mode='w')
    file_handler.setLevel(log_level)
    formatter = logging.Formatter(log_format)
    file_handler.setFormatter(formatter)
    return file_handler



def create_console_handler(coloured: bool, 
                           log_level: int, 
                           detailed_logs: bool, 
                           detailed_log_format: str, 
                           simple_log_format: str
                           ) -> logging.StreamHandler:
    
    console_handler = logging.StreamHandler()
    
    if coloured:
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
        coloredlogs.install(level=log_level)
    else:
        console_formatter = logging.Formatter(detailed_log_format) if detailed_logs else logging.Formatter(simple_log_format)
    
    console_handler.setFormatter(console_formatter)
    return console_handler



def log_event(logger: logging.Logger, level: int, message: str) -> None:
    logger.log(level, message)






# ================================================ CONFIG ================================================

def create_config(aws_access_key: str, 
                  aws_secret_key: str, 
                  aws_region_name: str, 
                  aws_s3_bucket: str, 
                  aws_s3_folder: str, 
                  local_target_path: str, 
                  s3_client: Any, 
                  WRITE_FILES_TO_CLOUD: bool
                  ) -> dict:
    
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

def create_webdriver(options: webdriver.ChromeOptions, service: Service) -> webdriver.Chrome:
    return webdriver.Chrome(options=options, service=service)
    

def load_webpage(chrome_driver: webdriver.Chrome, url: str, logger: logging.Logger) -> webdriver.Chrome:
    log_event(logger, logging.DEBUG, ">>> Loading webpage using Selenium ...")
    chrome_driver.get(url)
    sleep(3)
    return chrome_driver


def check_page_title(chrome_driver: webdriver.Chrome, expected_title: str, logger: logging.Logger) -> None:
    assert expected_title in chrome_driver.title, f"ERROR: Unable to load site for {expected_title} ... "
    log_event(logger, logging.DEBUG, ">>> Webpage successfully loaded ...")


def load_league_table(url: str, logger: logging.Logger, options: webdriver.ChromeOptions, service: Service, title_check: str) -> webdriver.Chrome:
    chrome_driver = create_webdriver(options, service)
    chrome_driver = load_webpage(chrome_driver, url, logger)
    check_page_title(chrome_driver, title_check, logger)
    
    return chrome_driver




# ================================================ POPUP HANDLER ================================================

def find_popup_element(chrome_driver: webdriver.Chrome, timeout: int) -> WebElement:
    wait = WebDriverWait(chrome_driver, timeout)

    return wait.until(EC.presence_of_element_located((By.XPATH, '/html/body/div[8]/div[2]/div[1]/div[1]/button/i')))


def close_popup(close_popup_box: WebElement, logger: logging.Logger) -> None:
    close_popup_box.click()
    log_event(logger, logging.DEBUG, f'>>>>   Closing cookie pop-up window ...')


def close_popup_box_for_table_standings_webpage(chrome_driver: webdriver.Chrome, logger: logging.Logger, timeout: int) -> None:
    try:
        close_popup_box = find_popup_element(chrome_driver, timeout)
        close_popup(close_popup_box, logger)
    except Exception as e:
        log_event(logger, logging.ERROR, f'No cookie pop-up window to close...let\'s begin scraping for league standings !!') 





# ================================================ DATA EXTRACTOR ================================================


def scrape_table_standings(chrome_driver: webdriver.Chrome, logger: logging.Logger) -> Optional[WebElement]:
    try:
        log_event(logger, logging.DEBUG, f'>>>>   Detecting HTML elements ...')
        return chrome_driver.find_element(By.CLASS_NAME, 'leaguetable')
    
    except Exception as e:
        log_event(logger, logging.ERROR, e)
        return None



def scrape_table_rows(table: Optional[WebElement], logger: logging.Logger)  -> List[WebElement]:
    log_event(logger, logging.DEBUG, f'>>>>   Extracting content from HTML elements ...')
    return table.find_elements(By.XPATH, './/tr') if table else []



def scrape_data_from_cells(table_row: WebElement, logger: logging.Logger, row_counter: int) -> List[str]:
    cells       =   table_row.find_elements(By.TAG_NAME, 'td')
    cell_data   =   [cell.text for cell in cells]
    for i, data in enumerate(cell_data):
        log_event(logger, logging.DEBUG, f'>>>>   Table row no "{row_counter}", Cell no "{i+1}" appended ...')
    return cell_data



def scrape_data_from_rows(table_rows: List[WebElement], logger: logging.Logger) -> List[List[str]]:
    return [scrape_data_from_cells(table_row, logger, i+1) for i, table_row in enumerate(table_rows) ]



def extract_data(chrome_driver: webdriver.Chrome, logger: logging.Logger) -> Optional[List[List[str]]]:
    prem_league_table   =   scrape_table_standings(chrome_driver, logger)
    table_rows          =   scrape_table_rows(prem_league_table, logger)
    return scrape_data_from_rows(table_rows, logger)




# ================================================ DATA TRANSFORMER ================================================


def create_dataframe(scraped_data: List[List[str]], scraped_columns: List[str], match_date: str, logger: logging.Logger) -> Optional[pd.DataFrame]:
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

def create_s3_key(s3_folder: str, file_name: str, match_date: str, logger: logging.Logger) -> str:
    try:
        log_event(logger, logging.DEBUG, '>>>> Creating S3 key for Prem League table file ...')
        s3_key = f"{s3_folder}/{file_name}_{match_date}.csv"
        return s3_key
    
    except Exception as e:
        log_event(logger, logging.ERROR, e)



def create_csv_buffer(logger: logging.Logger) -> io.StringIO:
    try:
        log_event(logger, logging.DEBUG, f">>> Creating in-memory buffer ...")
        return io.StringIO()
    
    except Exception as e:
        log_event(logger, logging.ERROR, e)



def write_df_to_csv(df: pd.DataFrame, csv_buffer: io.StringIO, logger: logging.Logger) -> None:
    try:
        log_event(logger, logging.DEBUG, f">>> Persisting dataframe to CSV file ...")
        df.to_csv(csv_buffer, index=False)

    except Exception as e:
        log_event(logger, logging.ERROR, e)



def get_string_values_from_buffer(csv_buffer: io.StringIO, logger: logging.Logger) -> str:
    try:
        log_event(logger, logging.DEBUG, f">>> Retrieving data from CSV buffer & storing as string values ...")
        return csv_buffer.getvalue()
    
    except Exception as e:
        log_event(logger, logging.ERROR, e)



def upload_string_to_s3(s3_client: Any, s3_bucket: str, s3_key: str, string_values: str, logger: logging.Logger) -> None :
    try:
        log_event(logger, logging.DEBUG, f">>> Preparing to upload file to S3 bucket ...")
        s3_client.put_object(Bucket=s3_bucket, Key=s3_key, Body=string_values)

    except Exception as e:
        log_event(logger, logging.ERROR, e)



def upload_df_to_s3(df: pd.DataFrame, match_date: str, file_name: str, config: Dict[str, Any], logger: logging.Logger) -> None:
    try:
        log_event(logger, logging.DEBUG, f">>> Composing final operations to begin upload to cloud ...")
        S3_KEY                              =   create_s3_key(config["S3_FOLDER"], file_name, match_date, logger)
        CSV_BUFFER                          =   create_csv_buffer(logger)
        write_df_to_csv(df, CSV_BUFFER, logger)
        RAW_TABLE_ROWS_AS_STRING_VALUES     =   get_string_values_from_buffer(CSV_BUFFER, logger)
        
        try:
            upload_string_to_s3(s3_client=config["S3_CLIENT"], s3_bucket=config["S3_BUCKET"], s3_key=S3_KEY, string_values=RAW_TABLE_ROWS_AS_STRING_VALUES, logger=logger)
            log_event(logger, logging.DEBUG, f">>> Successfully written and loaded '{file_name}' file to cloud target location in S3 bucket... ")
        except Exception as e:
            log_event(logger, logging.ERROR, e)

    except Exception as e:
        log_event(logger, logging.ERROR, e)
    



# B. UPLOAD TO LOCAL MACHINE

def create_local_file_path(target_path: str, file_name: str, match_date: str, logger: logging.Logger) -> str:
    try:
        log_event(logger, logging.DEBUG, '>>>> Creating Prem League table filepath  ...')
        return f"{target_path}/{file_name}_{match_date}.csv"

    except Exception as e:
        log_event(logger, logging.ERROR, e)


def write_df_to_local_file(df: pd.DataFrame, file_path: str, logger: logging.Logger) -> None:
    try:
        log_event(logger, logging.DEBUG, f">>> Converting data frame to CSV ...")
        df.to_csv(file_path, index=False)

    except Exception as e:
        log_event(logger, logging.ERROR, e)



def upload_df_to_local_file(df: pd.DataFrame, match_date: str, file_name: str, config: Dict[str, Any], logger: logging.Logger) -> None:
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






# Instantiate the functions in this script

if __name__=="__main__":

    # Set up constants to read into functions 

    match_date                      =   datetime.now().strftime('%Y-%b-%d') # for today's date
    football_url                    =   f'https://www.twtd.co.uk/league-tables/competition:premier-league/daterange/fromdate:2022-Jul-01/todate:{match_date}/type:home-and-away/'

    logger_name                     =   "football_scraper_fp"
    local_filepath                  =   'main_scraper'
    log_format                      =   '%(asctime)s | %(levelname)s | %(message)s'
    log_folder                      =   'logs/scraper/'

    coloured                        =   False
    detailed_logs                   =   False
    detailed_log_format             =   '%(asctime)s | %(levelname)s | %(message)s'
    simple_log_format               =   '%(message)s'
    webdriver_timeout               =   5
    WRITE_FILES_TO_CLOUD            =   False
    s3_client                       =   "s3"
    options                         =   webdriver.ChromeOptions()
    service                         =   Service(executable_path=ChromeDriverManager().install())
    title_check                     =   "Premier League"



    # Set up logging to file and console

    log_level           =   logging.DEBUG
    logger              =   create_logger(logger_name, log_level)
    file_handler        =   create_file_handler(local_filepath, log_level, log_format, log_folder)
    console_handler     =   create_console_handler(coloured, log_level, detailed_logs, detailed_log_format, simple_log_format) 
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)



    # Set up credentials for configuration 

    load_dotenv()

    aws_access_key          =   os.getenv("AWS_ACCESS_KEY")
    aws_secret_key          =   os.getenv("AWS_SECRET_KEY")
    aws_region_name         =   os.getenv("S3_REGION") 
    aws_s3_bucket           =   os.getenv("S3_BUCKET") 
    aws_s3_folder           =   os.getenv("S3_FOLDER") 
    local_target_path       =   os.getenv("LOCAL_TARGET_PATH") 
    config                  =   create_config(aws_access_key, aws_secret_key, aws_region_name, aws_s3_bucket, aws_s3_folder, local_target_path, s3_client, WRITE_FILES_TO_CLOUD=False)




    # Set up Selenium Chrome driver and configuration settings  
    chrome_driver = load_league_table(url=football_url, logger=logger, options=options, service=service, title_check=title_check)



    # Close popup box on webpage
    close_popup_box_for_table_standings_webpage(chrome_driver, timeout=webdriver_timeout, logger=logger)



    # Extract data (E)
    scraped_content          =   extract_data(chrome_driver, logger)
    


    # Transform data (T)
    prem_league_df           =   transform_data(scraped_content, match_date, logger)



    # Load data (L)
    file_name = "prem_league_table"
    config["WRITE_FILES_TO_CLOUD"] = False

    if config["WRITE_FILES_TO_CLOUD"]:
        upload_df_to_s3(prem_league_df, match_date, file_name, config, logger)

    else:
        upload_df_to_local_file(prem_league_df, match_date, file_name, config, logger)



    # Close driver when scraping is completed 
    chrome_driver.quit()