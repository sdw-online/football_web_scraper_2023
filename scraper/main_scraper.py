import io
import os
import boto3
from typing import List
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
class ILogger(ABC):

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


class FileLogger(ILogger):
    def __init__(self, local_filepath: str = Path(__file__).stem, log_format: str='%(asctime)s | %(levelname)s | %(message)s', level=logging.DEBUG):
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(level)

        self.file_handler = logging.FileHandler('logs/scraper/' +  local_filepath + '.log', mode='w')
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



class ConsoleLogger(ILogger):

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


class ColouredConsoleLogger(ConsoleLogger):
    def __init__(self, coloured: bool =True, level=logging.DEBUG):
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(level)
        self.console_handler = logging.StreamHandler()
        self.console_formatter = coloredlogs.ColoredFormatter(fmt    =   '%(message)s', level_styles=dict(
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
        self.coloured = coloured
        self.console_handler.setFormatter(self.console_formatter)
        coloredlogs.install(level=level)

        if __name__=="__main__":
            self.logger.addHandler(self.console_handler)

        

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




class NonColouredConsoleLogger(ConsoleLogger):
    def __init__(self, detailed_logs: bool= False, level=logging.DEBUG):
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(level)
        self.console_handler = logging.StreamHandler()

        self.logger.addHandler(self.console_handler)

        self.detailed_logs = detailed_logs
        if self.detailed_logs:
            detailed_log_format: str='%(asctime)s | %(levelname)s | %(message)s'
            self.console_formatter = logging.Formatter(detailed_log_format)
            self.console_handler.setFormatter(self.console_formatter)
        else:
            simple_log_format: str='%(message)s'
            self.console_formatter = logging.Formatter(simple_log_format)
            self.console_handler.setFormatter(self.console_formatter)
        

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
    def __init__(self, WRITE_FILES_TO_CLOUD: bool = False):
        self._AWS_ACCESS_KEY             =   os.getenv("ACCESS_KEY")
        self._AWS_SECRET_KEY             =   os.getenv("SECRET_ACCESS_KEY")
        self._S3_REGION                  =   os.getenv("REGION_NAME")
        self._S3_BUCKET                  =   os.getenv("S3_BUCKET")
        self._S3_FOLDER                  =   os.getenv("S3_FOLDER")
        self.LOCAL_TARGET_PATH           =   os.getenv("LOCAL_TARGET_PATH")

        # Set up constants for S3 file to be imported
        self.S3_CLIENT                  =   boto3.client('s3', aws_access_key_id=self._AWS_ACCESS_KEY, aws_secret_access_key=self._AWS_SECRET_KEY, region_name=self._S3_REGION)
        
        # Add a flag for saving CSV files to the cloud 
        self.WRITE_FILES_TO_CLOUD = WRITE_FILES_TO_CLOUD



# ================================================ WEBPAGE LOADER ================================================

class IWebPageLoader(ABC):
    @abstractmethod
    def load_page(self, url:str):
        pass

class WebPageLoader(IWebPageLoader):
    @abstractmethod
    def load_page(self, url:str):
        pass


class PremLeagueTableWebPageLoader(WebPageLoader):
    def __init__(self, options=None, service=None, coloured_console_logs: bool=False):
        if options is None:
            options = webdriver.ChromeOptions()
        if service is None:
            service = Service(executable_path=ChromeDriverManager().install())

        self.options = options
        self.service = service
        self.chrome_driver = webdriver.Chrome(service=self.service, options=self.options)
        self.coloured_console_logs = coloured_console_logs
        if coloured_console_logs:
            self.console_logger = ColouredConsoleLogger()
        else:
            self.console_logger = NonColouredConsoleLogger()


    def load_page(self, url: str):
        webpage_title = 'Premier League'
        self.console_logger.log_event_as_debug(">>> Loading webpage using Selenium ...")
        self.chrome_driver.get(url)
        sleep(3)
        
        # Check if webpage loaded successfully 
        assert webpage_title in self.chrome_driver.title, f"ERROR: Unable to load site for {webpage_title} ... "
        self.console_logger.log_event_as_debug(">>> Webpage successfully loaded ...")


class BundesligaTableWebPageLoader(WebPageLoader):
    pass


class LaligaTableWebPageLoader(WebPageLoader):
    pass


class SerieATableWebPageLoader(WebPageLoader):
    pass


class Ligue1TableWebPageLoader(WebPageLoader):
    pass


# ================================================ POPUP HANDLER ================================================

class IPopUpHandler(ABC):
    @abstractmethod
    def close_popup(self):
        pass


class PopUpHandler(IPopUpHandler):
    @abstractmethod
    def close_popup(self):
        pass


class PremLeagueTablePopUpHandler(PopUpHandler):

    def __init__(self, chrome_driver: webdriver.Chrome, logger: logging.Logger, coloured_console_logs: bool=False, file_logger=FileLogger()):
        self.chrome_driver = chrome_driver
        self.file_logger = file_logger  
        
        self.coloured_console_logs = coloured_console_logs
        if coloured_console_logs:
            self.console_logger = ColouredConsoleLogger()
        else:
            self.console_logger = NonColouredConsoleLogger()
        
        self.logger = logger
        self.logger.propagate = True
        
        

    

    def close_popup(self):
        try:
            wait = WebDriverWait(self.chrome_driver, 5)
            close_popup_box = wait.until(EC.presence_of_element_located((By.XPATH, '/html/body/div[8]/div[2]/div[1]/div[1]/button/i')))
            close_popup_box.click()
            self.console_logger.log_event_as_debug(f'>>>>   Closing cookie pop-up window ...')
        
        except Exception as e:
            self.console_logger.log_event_as_error(f'No cookie pop-up window to close...let\'s begin scraping for league standings !!')


class BundesligaTablePopUpHandler(PopUpHandler):
    pass


class LaLigaTablePopUpHandler(PopUpHandler):
    pass


class SerieATablePopUpHandler(PopUpHandler):
    pass


class Ligue1TablePopUpHandler(PopUpHandler):
    pass


# ================================================ DATA EXTRACTOR ================================================

class IDataExtractor(ABC):
    @abstractmethod
    def scrape_data(self):
        pass


class TableStandingsDataExtractor(IDataExtractor):
    @abstractmethod
    def scrape_data(self):
        pass



class PremLeagueTableStandingsDataExtractor(TableStandingsDataExtractor):

    def __init__(self, chrome_driver: webdriver.Chrome, match_date: str, coloured_console_logs: bool=False, file_logger=FileLogger()):
        self.chrome_driver = chrome_driver
        self.match_date = match_date
        self.file_logger = file_logger
        self.coloured_console_logs = coloured_console_logs
        if self.coloured_console_logs:
            self.console_logger = ColouredConsoleLogger()
        else:
            self.console_logger = NonColouredConsoleLogger()
    
    def scrape_data(self):
        try:
            table                   =   self.chrome_driver.find_element(By.CLASS_NAME, 'leaguetable')
            table_rows              =   table.find_elements(By.XPATH, './/tr')
            table_row_counter       =   0
            scraped_content         =   []
            self.console_logger.log_event_as_debug(f'>>>>   Extracting content from HTML elements ...')

            for table_row in table_rows:
                table_row_counter   +=  1
                self.console_logger.log_event_as_debug(f'>>>>>>>   Table no {table_row_counter} <<<<<<  ')
                cells           =   table_row.find_elements(By.TAG_NAME, 'td')
                row_data        =   []
                cell_counter    =   0

                for cell in cells:
                    cell_counter += 1
                    row_data.append(cell.text)
                    self.file_logger.log_event_as_debug(f'>>>>   Table row no "{table_row_counter}", Cell no "{cell_counter}" appended ...')
                    self.file_logger.log_event_as_debug(f'>>>>   ')

                    print(cell.text)

                scraped_content.append(row_data)
        except Exception as e:
            self.console_logger.log_event_as_error(e)

        return scraped_content



class BundesligaTableStandingsDataExtractor(TableStandingsDataExtractor):
    pass


class LaligaTableStandingsDataExtractor(TableStandingsDataExtractor):
    pass


class SerieATableStandingsDataExtractor(TableStandingsDataExtractor):
    pass


class Ligue1TableStandingsDataExtractor(TableStandingsDataExtractor):
    pass


# ================================================ DATA TRANSFORMER ================================================


class IDataTransformer(ABC):
    @abstractmethod
    def transform_data(self, scraped_content: List[List[str]], match_date: str) -> pd.DataFrame:
        pass



class TableStandingsDataTransformer(IDataTransformer):
    @abstractmethod
    def transform_data(self, scraped_content: List[List[str]], match_date: str) -> pd.DataFrame:
        pass




class PremierLeagueTableStandingsDataTransformer(TableStandingsDataTransformer):
    def __init__(self, coloured_console_logs: bool=False, file_logger=FileLogger()):
        self.file_logger = file_logger
        self.coloured_console_logs = coloured_console_logs
        if self.coloured_console_logs:
            self.console_logger = ColouredConsoleLogger()
        else:
            self.console_logger = NonColouredConsoleLogger()

    def transform_data(self, scraped_content: List[List[str]], match_date: str) -> pd.DataFrame:
        try:
            self.file_logger.log_event_as_debug(f'>>>> Transforming scraped Premier League content...')
            
            scraped_data            =   scraped_content[1:]
            scraped_columns         =   scraped_content[0]

            table_df                =   pd.DataFrame(data=scraped_data, columns=scraped_columns)
            table_df['match_date']  =   match_date


            self.file_logger.log_event_as_debug(f'>>>> Successfully transformed Premier League content...')
        except Exception as e:
            self.console_logger.log_event_as_error(e)

        return table_df


class BundesligaTableStandingsDataTransformer(TableStandingsDataTransformer):
        pass


class LaligaTableStandingsDataTransformer(TableStandingsDataTransformer):
        pass


class SerieATableStandingsDataTransformer(TableStandingsDataTransformer):
        pass


class Ligue1TableStandingsDataTransformer(TableStandingsDataTransformer):
        pass


# ================================================ DATA UPLOADER ================================================


class IFileUploader(ABC):

    @abstractmethod
    def upload_file(self):
        pass



class S3FileUploader(IFileUploader):
    @abstractmethod
    def upload_file(self):
        pass


class S3CSVFileUploader(S3FileUploader):
    @abstractmethod
    def upload_file(self):
        pass



class PremierLeagueTableS3CSVUploader(S3CSVFileUploader):

    def __init__(self, coloured_console_logs: bool=False, file_logger=FileLogger(), cfg: Config = Config(WRITE_FILES_TO_CLOUD=True)):
        self.cfg                    =   cfg
        self.s3_client: str         =   self.cfg.S3_CLIENT
        self.s3_bucket: str         =   self.cfg._S3_BUCKET
        self.s3_folder: str         =   self.cfg._S3_FOLDER
        self.s3_region: str         =   self.cfg._S3_REGION

        self.file_logger            =   file_logger
        self.coloured_console_logs  =   coloured_console_logs
        if self.coloured_console_logs:
            self.console_logger = ColouredConsoleLogger()
        else:
            self.console_logger = NonColouredConsoleLogger()

        self.file_name_prefix: str = 'prem_league_table'



    def upload_file(self, prem_league_df: pd.DataFrame, match_date: str):

        if self.cfg.WRITE_FILES_TO_CLOUD:
            try:
                self.console_logger.log_event_as_debug(f">>> Saving Prem League table file into S3 folder ...")
                S3_KEY = f"{self.s3_folder}/{self.file_name_prefix}_{match_date}.csv"

                
                self.console_logger.log_event_as_debug(f">>> Creating in-memory buffer ...")
                CSV_BUFFER = io.StringIO()

                self.console_logger.log_event_as_debug(f">>> Persisting dataframe to CSV file ...")
                prem_league_df.to_csv(CSV_BUFFER, index=False)

                
                self.console_logger.log_event_as_debug(f">>> Retrieving data from CSV buffer & storing as string values ...")
                RAW_TABLE_ROWS_AS_STRING_VALUES = CSV_BUFFER.getvalue()
                self.s3_client.put_object(Bucket=self.s3_bucket, Key=S3_KEY, Body=RAW_TABLE_ROWS_AS_STRING_VALUES)
            
                self.console_logger.log_event_as_debug(f"")
                self.console_logger.log_event_as_debug(f">>> Successfully written and loaded '{self.file_name_prefix}' file to cloud target location in S3 bucket... ")
                self.console_logger.log_event_as_debug(f"")

            except Exception as e:
                self.console_logger.log_event_as_warning(e)
        else:
            self.file_logger.log_event_as_error(">>> Unable to upload to S3 bucket: Set 'WRITE_FILES_TO_CLOUD' to 'True' to upload files to S3 bucket.")
            raise ImportError("Unable to upload to S3 bucket: Set 'WRITE_FILES_TO_CLOUD' to 'True' to upload files to S3 bucket.")


class BundesligaTableS3CSVUploader(S3CSVFileUploader):
    pass


class LaligaTableS3CSVUploader(S3CSVFileUploader):
    pass


class SerieATableS3CSVUploader(S3CSVFileUploader):
    pass


class Ligue1TableS3CSVUploader(S3CSVFileUploader):
    pass






class S3JSONFileUploader(S3FileUploader):
    pass




class LocalFileUploader(IFileUploader):
    @abstractmethod
    def upload_file(self):
        pass


class PremierLeagueTableLocalCSVUploader(LocalFileUploader):
    
    def __init__(self, target_path: str=None, file_name: str='prem_league_table', coloured_console_logs: bool=False, file_logger=FileLogger()):
        self.cfg = Config()
        
        if target_path is None:
            target_path = self.cfg.LOCAL_TARGET_PATH
            self.target_path = target_path

        self.file_name = file_name
        self.file_logger = file_logger
        self.coloured_console_logs = coloured_console_logs
        if self.coloured_console_logs:
            self.console_logger = ColouredConsoleLogger()
        else:
            self.console_logger = NonColouredConsoleLogger()

    def upload_file(self, prem_league_df: pd.DataFrame, match_date: str):
        try:
            self.console_logger.log_event_as_debug(f">>> Saving Prem League table file into local folder...")

            prem_league_table_file = f'{self.target_path}/{self.file_name}_{match_date}'
            prem_league_df.to_csv(f'{prem_league_table_file}.csv', index=False)
            
            self.console_logger.log_event_as_debug(f"")
            self.console_logger.log_event_as_debug(f">>> Successfully written and loaded '{self.file_name}' file to local target location... ")
            self.console_logger.log_event_as_debug(f"")
        except Exception as e:
            self.console_logger.log_event_as_error(e)


        prem_league_table_file = f'{self.target_path}/{self.file_name}_{match_date}'
        prem_league_df.to_csv(f'{prem_league_table_file}.csv', index=False)

        self.file_logger.log_event_as_debug(f"")
        self.file_logger.log_event_as_debug(f">>> Successfully written and loaded '{self.file_name}' file to local target location... ")
        self.file_logger.log_event_as_debug(f"")


class BundesligaTableLocalCSVUploader(LocalFileUploader):
    pass


class LaligaTableLocalCSVUploader(LocalFileUploader):
    pass


class SerieATableLocalCSVUploader(LocalFileUploader):
    pass


class Ligue1TableLocalCSVUploader(LocalFileUploader):
    pass




if __name__=="__main__":

    # Specify the constants for the scraper
    local_target_path               =   os.path.abspath('temp_storage/dirty_data')
    match_date                      =   '2023-Apr-23'
    football_url                    =   f'https://www.twtd.co.uk/league-tables/competition:premier-league/daterange/fromdate:2022-Jul-01/todate:{match_date}/type:home-and-away/'



    # Load environment variables to session
    load_dotenv()
    cfg = Config(WRITE_FILES_TO_CLOUD=True)

    


    # Load webpage 
    webpage_loader = PremLeagueTableWebPageLoader()
    webpage_loader.load_page(football_url)
    

    # Close popup boxes if they appear on webpage
    logger = logging.getLogger(__name__)
    popup_handler = PremLeagueTablePopUpHandler(webpage_loader.chrome_driver, logger)
    popup_handler.close_popup()


    # Extract data 
    data_extractor = PremLeagueTableStandingsDataExtractor(chrome_driver=webpage_loader.chrome_driver, match_date=match_date, coloured_console_logs=False)
    prem_league_scraped_data = data_extractor.scrape_data()
    

    # Transform data 
    data_transformer = PremierLeagueTableStandingsDataTransformer(coloured_console_logs=False)
    df = data_transformer.transform_data(scraped_content=prem_league_scraped_data, match_date=match_date)
    print(df)


    # Load data 
    
    if cfg.WRITE_FILES_TO_CLOUD:

        cloud_data_uploader = PremierLeagueTableS3CSVUploader(coloured_console_logs=False, cfg=cfg)
        cloud_data_uploader.upload_file(df, match_date=match_date)
    
    else:

        local_data_uploader = PremierLeagueTableLocalCSVUploader(coloured_console_logs=False)
        local_data_uploader.upload_file(df, match_date=match_date)

        
    # Close Selenium Chrome driver 
    webpage_loader.chrome_driver.quit()
    