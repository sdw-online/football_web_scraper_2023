
import io
import os
import boto3
import pandas as pd
from time import sleep
from pathlib import Path
import logging, coloredlogs
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support import expected_conditions as EC
from abc import ABC, abstractmethod

# ================================================ LOGGER ================================================

# Set up root root_logger 
class Logger(ABC):

    @abstractmethod
    def log_event_as_debug(self, message: str):
        pass

    @abstractmethod
    def log_event_as_info(self, message: str):
        pass

    @abstractmethod
    def log_event_as_warning(self, message: str):
        pass
    
    @abstractmethod
    def log_event_as_critical(self, message: str):
        pass
    
    @abstractmethod
    def log_event_as_error(self, message: str):
        pass


class FileLogger(Logger):
    def __init__(self, file_path: str, log_format: str='%(asctime)s | %(levelname)s | %(message)s', level=logging.DEBUG):
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(level)

        self.file_handler = logging.FileHandler(file_path)
        self.file_handler.setLevel(level)
        self.formatter = logging.Formatter(log_format)
        self.file_handler.setFormatter(self.formatter)

        self.logger.addHandler(self.file_handler)
        

    def log_event_as_debug(self, message: str):
        self.logger.debug(message)

    def log_event_as_info(self, message: str):
        self.logger.info(message)

    def log_event_as_warning(self, message: str):
        self.logger.warning(message)

    def log_event_as_critical(self, message: str):
        self.logger.critical(message)
    
    def log_event_as_error(self, message: str):
        self.logger.error(message)



class ConsoleLogger(Logger):
    def __init__(self, coloured: bool =True, log_format: str='%(asctime)s | %(levelname)s | %(message)s', level=logging.DEBUG):
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(level)
        self.console_handler = logging.StreamHandler()
        self.formatter = logging.Formatter(log_format)
        self.console_handler.setFormatter(self.formatter)
        self.logger.addHandler(self.console_handler)
        self.coloured = coloured

        if self.coloured:
            coloredlogs.install(level=level)
        
        
    def log_event_as_debug(self, message: str):
        self.logger.debug(message)

    def log_event_as_info(self, message: str):
        self.logger.info(message)

    def log_event_as_warning(self, message: str):
        self.logger.warning(message)

    def log_event_as_critical(self, message: str):
        self.logger.critical(message)
    
    def log_event_as_error(self, message: str):
        self.logger.error(message)

     











# ================================================ CONFIG ================================================



# Set up environment variables
class Config:
    def __init__(self):
        self._AWS_ACCESS_KEY             =   os.getenv("ACCESS_KEY")
        self._AWS_SECRET_KEY             =   os.getenv("SECRET_ACCESS_KEY")
        self._S3_REGION                  =   os.getenv("REGION_NAME")
        self._S3_BUCKET                  =   os.getenv("S3_BUCKET")
        self._S3_FOLDER                  =   os.getenv("S3_FOLDER")

        # Set up constants for S3 file to be imported
        self.s3_client                  =   boto3.client('s3', aws_access_key_id=self._AWS_ACCESS_KEY, aws_secret_access_key=self._AWS_SECRET_KEY, region_name=self._S3_REGION)
        
        # Add a flag for saving CSV files to the cloud 
        self.WRITE_TO_CLOUD = False











# Set up the Selenium Chrome driver 


class Scraper(ABC):
    def __init__(self, options: webdriver.ChromeOptions(), service: Service(executable_path=ChromeDriverManager().install()) ):
        self.options = options
        self.service = service
        self.chrome_driver = webdriver.Chrome(service=self.service, options=self.options)

    @abstractmethod
    def load_page(self, url: str):
        pass

    @abstractmethod
    def close_popup(self):
        pass


    @abstractmethod
    def scrape_data(self):
        pass

class PremierLeagueScraper(Scraper):
    def __init__(self):
        super().__init__()
    
    def load_page(self, url: str):
        self.chrome_driver.get(url)

    
    def close_popup(self):
        try:
            wait = WebDriverWait(self.chrome_driver, 5)
            close_popup_box = wait.until(EC.presence_of_element_located((By.XPATH, '/html/body/div[8]/div[2]/div[1]/div[1]/button/i')))
            close_popup_box.click()
            ColouredLogger.write_to_console(f'>>>>   Closing cookie pop-up window ...')
        except Exception as e:
            ColouredLogger.write_to_console(f'No cookie pop-up window to close...let\'s begin scraping for league standings !!')

    
    def scrape_data(self):
        table = self.chrome_driver.find_element(By.CLASS_NAME, 'leaguetable')
        table_rows = table.find_elements(By.XPATH, './/tr')
        scraped_content = []

        for table_row in table_rows:
            cells = table_row.find_elements(By.TAG_NAME, 'td')
            row_data = [cell.text for cell in cells]
            scraped_content.append(row_data)

        ColouredLogger.write_to_console(f'>>>>   Extracting content from HTML elements ...')
        return scraped_content


class FileUploader(ABC):

    @abstractmethod
    def upload_file(self):
        pass

    

class S3FileUploader(FileUploader):
    def __init__(self, bucket: str, s3_client: str, folder: str):
        self.bucket = bucket
        self.s3_client = s3_client
        self.folder = folder
    

    def upload_file(self, file_name: str, file_content: str):
        key = f"{self.folder}/{file_name}"
        self.s3_client.put_object(Bucket=self.bucket, Key=key, Body=file_content)

        

class LocalFileUploader(FileUploader):
    def __init__(self, target_path: str, folder: str):
        self.target_path = target_path
        self.folder = folder
    
    def upload_file(self, file_name: str, file_content: str):
        with open(f'{self.target_path}/{self.folder}/{file_name}', "w") as file:
            file.write(file_content)


WRITE_TO_CLOUD = False



if __name__=="__main__":

# Load environment variables to session
load_dotenv()



# Specify the constants for the scraper 
local_target_path               =   os.path.abspath('temp_storage/dirty_data')
match_dates                     =   ['2023-Apr-17']
# match_dates                     =   ['2022-Sep-01', '2022-Oct-01', '2022-Nov-01', '2022-Dec-01', '2023-Jan-01', '2023-Feb-01', '2023-Mar-01', '2023-Mar-07', '2023-Mar-08', '2023-Mar-12']
# match_dates                     =   ['2022-Sep-01', '2023-Mar-07']
table_counter                   =   0
   




if WRITE_TO_CLOUD:
    uploader = S3FileUploader(S3_BUCKET, s3_client, S3_FOLDER)
else:
    uploader = LocalFileUploader(local_target_path, LOCAL_FOLDER)

prem_league_table_df.to_csv(CSV_BUFFER, index=False)
file_content = CSV_BUFFER.getvalue()
uploader.upload(prem_league_table_file, file_content)
