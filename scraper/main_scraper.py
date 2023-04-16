
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
class Logger:
    def __init__(self, logger_name=__name__):
        self.logger = logging.getLogger(logger_name)
        self.logger.setLevel(logging.DEBUG)

        # Set up formatter for logs
        self.file_handler_formatter = logging.Formatter('%(asctime)s  |  %(levelname)s  |  %(message)s  ')
        self.console_handler_log_formatter =  coloredlogs.ColoredFormatter(fmt    =   '%(message)s', level_styles=dict(
                                                                                                debug           =   dict    (color  =   'white'),
                                                                                                info            =   dict    (color  =   'green'),
                                                                                                warning         =   dict    (color  =   'cyan'),
                                                                                                error           =   dict    (color  =   'red',      bold    =   True,   bright      =   True),
                                                                                                critical        =   dict    (color  =   'black',    bold    =   True,   background  =   'red')
                                                                                            ),

                                                                                    field_styles=dict(
                                                                                        messages            =   dict    (color  =   'white')
                                                                                    )
                                                                                    )
        # Set up file handler object for logging events to file
        self.file_handler = logging.FileHandler('logs/scraper/' + Path(__file__).stem + '.log', mode='w')
        self.file_handler.setFormatter(self.file_handler_formatter)
        
        # Set up console handler object for writing event logs to console in real time (i.e. streams events to stderr)
        self.console_handler = logging.StreamHandler()


    @abstractmethod
    def write_to_file():
        pass

    @abstractmethod
    def print_to_console():
        pass


class ColouredLogger(Logger):
    def __init__(self, logger_name=__name__):
        super().__init__(logger_name)
        self.console_handler.setFormatter(self.console_handler_log_formatter)
        self.logger.addHandler(self.file_handler)
        self.logger.addHandler(self.console_handler)
    
    def write_to_file(self, log_message):
        self.logger.critical(log_message)

    def print_to_console(self, log_message):
        self.logger.warning(log_message)



class NonColouredLogger(Logger):
    def __init__(self, logger_name=__name__):
        super().__init__(logger_name)
        self.console_handler.setFormatter(logging.Formatter('%(asctime)s | %(levelname)s | %(message)s'))
        self.logger.addHandler(self.file_handler)
        self.logger.addHandler(self.console_handler)


    def write_to_file(self, log_message):
        self.logger.debug(log_message)

    def print_to_console(self, log_message):
        self.logger.info(log_message)








# ================================================ CONFIG ================================================



# Set up environment variables
class ConfigInterface:
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







# Load environment variables to session
load_dotenv()



# Specify the constants for the scraper 
local_target_path               =   os.path.abspath('temp_storage/dirty_data')
match_dates                     =   ['2023-Apr-16']
# match_dates                     =   ['2022-Sep-01', '2022-Oct-01', '2022-Nov-01', '2022-Dec-01', '2023-Jan-01', '2023-Feb-01', '2023-Mar-01', '2023-Mar-07', '2023-Mar-08', '2023-Mar-12']
# match_dates                     =   ['2022-Sep-01', '2023-Mar-07']
table_counter                   =   0



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
            ColouredLogger.print_to_console(f'>>>>   Closing cookie pop-up window ...')
        except Exception as e:
            ColouredLogger.print_to_console(f'No cookie pop-up window to close...let\'s begin scraping for league standings !!')

    
    def scrape_data(self):
        table = self.chrome_driver.find_element(By.CLASS_NAME, 'leaguetable')
        table_rows = table.find_elements(By.XPATH, './/tr')
        scraped_content = []

        for table_row in table_rows:
            cells = table_row.find_elements(By.TAG_NAME, 'td')
            row_data = [cell.text for cell in cells]
            scraped_content.append(row_data)

        ColouredLogger.print_to_console(f'>>>>   Extracting content from HTML elements ...')
        return scraped_content









# Begin scraping 
for match_date in match_dates:
    try:
        prem_league_table_url   =   f'https://www.twtd.co.uk/league-tables/competition:premier-league/daterange/fromdate:2022-Jul-01/todate:{match_date}/type:home-and-away/'
        chrome_driver.get(prem_league_table_url)
        root_logger.info(f'>>>>   Running Prem League link for league standings as of {match_date} ...')
        root_logger.debug(f'>>>>   ')
        try:
            wait = WebDriverWait(chrome_driver, 5)
            close_cookie_box    =   wait.until(EC.presence_of_element_located((By.XPATH, '/html/body/div[8]/div[2]/div[1]/div[1]/button/i')))
            close_cookie_box.click()
            root_logger.info(f'>>>>   Closing cookie pop-up window ...')
            root_logger.debug(f'>>>>   ')
        except Exception as e:
            root_logger.info(f'No cookie pop-up window to close...let\'s begin scraping for league standings as of "{match_date}"  !!')

        table                   =   chrome_driver.find_element(By.CLASS_NAME, 'leaguetable')
        table_rows              =   table.find_elements(By.XPATH, './/tr')
        table_row_counter       =   0
        scraped_content         =   []


        root_logger.info(f'>>>>   Extracting content from HTML elements ...')
        root_logger.debug(f'>>>>   ')


        for table_row in table_rows:
            table_row_counter   +=  1
            root_logger.debug(f'>>>>>>>   Table no {table_counter} <<<<<<  ')
            root_logger.debug(f'>>>>   ')
            cells           =   table_row.find_elements(By.TAG_NAME, 'td')
            row_data        =   []
            cell_counter    =   0
            for cell in cells:
                cell_counter += 1
                row_data.append(cell.text)
                root_logger.debug(f'>>>>   Table row no "{table_row_counter}", Cell no "{cell_counter}" appended ...')
                root_logger.debug(f'>>>>   ')
            scraped_content.append(row_data)


        # Use HTML content to create data frame for the Premier League table standings 
        prem_league_table_df                    =   pd.DataFrame(data=scraped_content[1:], columns=[scraped_content[0]])
        prem_league_table_df['match_date']      =   match_date
        prem_league_table_file                  =   f'prem_league_table_{match_date}.csv'
        
        S3_KEY                                       =       S3_FOLDER + prem_league_table_file
        CSV_BUFFER                                   =       io.StringIO()


        root_logger.debug(prem_league_table_df)

        # Write data frame to CSV file


        if WRITE_TO_CLOUD:
            prem_league_table_df.to_csv(CSV_BUFFER, index=False)
            RAW_TABLE_ROWS_AS_STRING_VALUES              =       CSV_BUFFER.getvalue()

            # Load Postgres table to S3
            s3_client.put_object(Bucket=S3_BUCKET,
                        Key=S3_KEY,
                        Body=RAW_TABLE_ROWS_AS_STRING_VALUES
                        )
            root_logger.info("")
            root_logger.info(f'>>>>   Successfully written and loaded "{prem_league_table_file}" file to the "{S3_BUCKET}" S3 bucket target location...')
            root_logger.debug(f'>>>>   ')

        else:
            prem_league_table_df.to_csv(f'{local_target_path}/{prem_league_table_file}', index=False)
            root_logger.info("")
            root_logger.info(f'>>>>   Successfully written and loaded "{prem_league_table_file}" file to local target location...')
            root_logger.debug(f'>>>>   ')



        # Add delays to avoid overloading the website's servers 
        sleep(3)




    except Exception as e:
        root_logger.error(e)


chrome_driver.quit()