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
        if __name__=="__main__":
            self.logger.addHandler(self.console_handler)

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

 

# # ================================================ CONFIG ================================================



# # Set up environment variables
# class Config:
#     def __init__(self, WRITE_FILES_TO_CLOUD: bool = False):
#         self._AWS_ACCESS_KEY             =   os.getenv("ACCESS_KEY")
#         self._AWS_SECRET_KEY             =   os.getenv("SECRET_ACCESS_KEY")
#         self._S3_REGION                  =   os.getenv("REGION_NAME")
#         self._S3_BUCKET                  =   os.getenv("S3_BUCKET")
#         self._S3_FOLDER                  =   os.getenv("S3_FOLDER")

#         # Set up constants for S3 file to be imported
#         self.S3_CLIENT                  =   boto3.client('s3', aws_access_key_id=self._AWS_ACCESS_KEY, aws_secret_access_key=self._AWS_SECRET_KEY, region_name=self._S3_REGION)
        
#         # Add a flag for saving CSV files to the cloud 
#         self.WRITE_FILES_TO_CLOUD = WRITE_FILES_TO_CLOUD



# # ================================================ WEBPAGE LOADER ================================================

# class IWebPageLoader(ABC):
#     @abstractmethod
#     def load_page(self, url:str):
#         pass

# class WebPageLoader(IWebPageLoader):
#     def __init__(self, options: webdriver.ChromeOptions(), service: Service(executable_path=ChromeDriverManager().install())):
#         self.options = options
#         self.service = service
#         self.chrome_driver = webdriver.Chrome(service=self.service, options=self.options)

    
#     def load_page(self, url: str):
#         self.chrome_driver.get(url)




# # ================================================ POPUP HANDLER ================================================

# class IPopUpHandler(ABC):
#     @abstractmethod
#     def close_popup(self):
#         pass


# class PopUpHandler(IPopUpHandler):

#     file_logger = FileLogger()
#     console_logger = ConsoleLogger()

#     def __init__(self, chrome_driver: webdriver.Chrome):
#         self.chrome_driver = chrome_driver
    

#     def close_popup(self):
#         try:
#             wait = WebDriverWait(self.chrome_driver, 5)
#             close_popup_box = wait.until(EC.presence_of_element_located((By.XPATH, '/html/body/div[8]/div[2]/div[1]/div[1]/button/i')))
#             close_popup_box.click()
#             self.file_logger.log_event_as_debug(f'>>>>   Closing cookie pop-up window ...')
#             self.console_logger.log_event_as_debug(f'>>>>   Closing cookie pop-up window ...')
        
#         except Exception as e:
#             self.file_logger.log_event_as_debug(f'No cookie pop-up window to close...let\'s begin scraping for league standings !!')
#             self.console_logger.log_event_as_debug(f'No cookie pop-up window to close...let\'s begin scraping for league standings !!')




# # ================================================ EXTRACTOR ================================================

# class IDataExtractor(ABC):
#     @abstractmethod
#     def scrape_data(self):
#         pass


# class TableStandingsDataExtractor(IDataExtractor):
#     @abstractmethod
#     def scrape_data(self):
#         pass



# class PremLeagueTableStandingsDataExtractor(TableStandingsDataExtractor):

#     file_logger = FileLogger()
#     console_logger = ConsoleLogger()


#     def __init__(self, chrome_driver: webdriver.Chrome, match_date: str):
#         self.chrome_driver = chrome_driver
#         self.match_date = match_date
    
#     def scrape_data(self):
#         table = self.chrome_driver.find_element(By.CLASS_NAME, 'leaguetable')
#         table_rows = table.find_elements(By.XPATH, './/tr')
#         scraped_content = []

#         for table_row in table_rows:
#             cells = table_row.find_elements(By.TAG_NAME, 'td')
#             row_data = [cell.text for cell in cells]
#             scraped_content.append(row_data)
#             sleep(1.5)

#         return scraped_content




# # ================================================ SCRAPER ================================================


# class IScraper(ABC):
#     def __init__(self, webpage_loader: IWebPageLoader, popup_handler: IPopUpHandler, data_extractor: IDataExtractor):
#         self.webpage_loader = webpage_loader
#         self.popup_handler = popup_handler
#         self.data_extractor = data_extractor

    
#     @abstractmethod
#     def scrape(self):
#         pass



# class PremierLeagueTableScraper(IScraper):
#     file_logger = FileLogger()
#     console_logger = ConsoleLogger()
#     match_date: str = '2023-Apr-20'


#     def __init__(self, webpage_loader: IWebPageLoader, popup_handler: IPopUpHandler, data_extractor: PremLeagueTableStandingsDataExtractor):
#         super().__init__(webpage_loader, popup_handler, data_extractor)
#         self.match_date = match_date
    
#     def scrape(self, url: str = f'https://www.twtd.co.uk/league-tables/competition:premier-league/daterange/fromdate:2022-Jul-01/todate:{match_date}/type:home-and-away/'):
#         self.webpage_loader.load_page(url)
#         self.popup_handler.close_popup()
#         scraped_content = self.data_extractor.scrape_data()
#         self.file_logger.log_event_as_debug(">>>    Scraping Premier League table standings completed! ")
#         self.console_logger.log_event_as_debug(">>>    Scraping Premier League table standings completed! ")
        
#         return scraped_content
        

# class BundesligaTableScraper(IScraper):
#     pass


# class LaLigaTableScraper(IScraper):
#     pass


# class SerieATableScraper(IScraper):
#     pass


# class Ligue1TableScraper(IScraper):
#     pass



# # ================================================ DATA TRANSFORMER ================================================


# class IDataTransformer(ABC):
#     @abstractmethod
#     def transform_data(self, scraped_content: List[List[str]], match_date: str) -> pd.DataFrame:
#         pass



# class TableStandingsDataTransformer(IDataTransformer):
#     @abstractmethod
#     def transform_data(self, scraped_content: List[List[str]], match_date: str) -> pd.DataFrame:
#         pass




# class PremierLeagueTableStandingsDataTransformer(TableStandingsDataTransformer):
#     file_logger = FileLogger()
#     console_logger = ConsoleLogger()


#     def transform_data(self, scraped_content: List[List[str]], match_date: str = '2023-Apr-20') -> pd.DataFrame:
#         self.file_logger.log_event_as_debug(f'>>>> Extracting content from HTML elements...')
#         self.console_logger.log_event_as_debug(f'>>>> Extracting content from HTML elements...')
        
#         scraper = PremierLeagueTableScraper()
#         scraped_data = scraper.scrape()

#         table_df = pd.DataFrame(data=scraped_content[1:], columns=[scraped_content[0]])
#         table_df['match_date'] = match_date

#         return table_df


# class BundesligaTableStandingsDataTransformer(TableStandingsDataTransformer):
#         pass


# class LaligaTableStandingsDataTransformer(TableStandingsDataTransformer):
#         pass


# class SerieATableStandingsDataTransformer(TableStandingsDataTransformer):
#         pass


# class Ligue1TableStandingsDataTransformer(TableStandingsDataTransformer):
#         pass


# # ================================================ UPLOADER ================================================


# class IFileUploader(ABC):
#     file_logger = FileLogger()
#     console_logger = ConsoleLogger()

#     @abstractmethod
#     def upload_file(self):
#         pass



# class S3FileUploader(IFileUploader):
#     @abstractmethod
#     def upload_file(self):
#         pass


# class S3CSVFileUploader(S3FileUploader):
#     @abstractmethod
#     def upload_file(self):
#         pass



# class S3CSVPremierLeagueTableStandingsUploader(S3CSVFileUploader):
#     cfg = Config()
#     prem_league_df = PremierLeagueTableStandingsDataTransformer.transform_data()
    

#     def __init__(self, s3_client: str = cfg.S3_CLIENT, s3_bucket: str = cfg._S3_BUCKET, s3_folder: str = cfg._S3_FOLDER, s3_region: str = cfg._S3_REGION):
#         self.s3_client: str = s3_client
#         self.s3_bucket: str = s3_bucket
#         self.s3_folder: str = s3_folder
#         self.s3_region: str = s3_region

#         self.file_name_prefix: str = 'prem_league_table'
#         self.file_format: str = 'csv'


#     def upload_file(self, table_standings_df: pd.DataFrame, match_date: str):

#         if self.cfg.WRITE_FILES_TO_CLOUD:
#             try:
#                 S3_KEY = f"{self.s3_folder}/{self.file_name_prefix}_{match_date}.{self.file_format}"
#                 CSV_BUFFER = io.StringIO()
#                 table_standings_df.to_csv(CSV_BUFFER, index=False)
#                 RAW_TABLE_ROWS_AS_STRING_VALUES = CSV_BUFFER.getvalue()

#                 self.s3_client.put_object(Bucket=self.s3_bucket, Key=S3_KEY, Body=RAW_TABLE_ROWS_AS_STRING_VALUES)

#             except Exception as e:
#                 self.file_logger.log_event_as_warning(e)
#                 self.console_logger.log_event_as_warning(e)
#         else:
#             self.file_logger.log_event_as_error(">>> Unable to upload to S3 bucket: Set 'WRITE_FILES_TO_CLOUD' to 'True' to upload files to S3 bucket.")
#             raise ImportError("Unable to upload to S3 bucket: Set 'WRITE_FILES_TO_CLOUD' to 'True' to upload files to S3 bucket.")



# class S3JSONFileUploader(S3FileUploader):
#     pass



# class LocalFileUploader(IFileUploader):
#     @abstractmethod
#     def upload_file(self):
#         pass


# class LocalCSVPremierLeagueTableStandingsUploader(LocalFileUploader):
#     cfg = Config()

#     def __init__(self, target_path: str):
#         self.target_path = target_path

#     def upload_file(self, file_name: str, prem_league_df: pd.DataFrame):
#         prem_league_table_file = f'{self.target_path}/{file_name}'

#         prem_league_df.to_csv(f'{prem_league_table_file}.csv', index=False)
#         self.file_logger.log_event_as_debug(f"")
#         self.file_logger.log_event_as_debug(f">>> Successfully written and loaded '{prem_league_table_file}' file to local target location... ")
#         self.file_logger.log_event_as_debug(f"")
            




if __name__=="__main__":

    # Specify the constants for the scraper
    local_target_path               =   os.path.abspath('temp_storage/dirty_data')
    match_date                      =   ['2023-Apr-20']
    football_url                    =   f'https://www.twtd.co.uk/league-tables/competition:premier-league/daterange/fromdate:2022-Jul-01/todate:{match_date}/type:home-and-away/'



    # Load environment variables to session
    load_dotenv()

    file_logger = FileLogger()
    console_logger = ConsoleLogger()



    console_logger.log_event_as_debug("This works !!! ")
    console_logger.log_event_as_info("This works !!! ")
    console_logger.log_event_as_warning("This works !!! ")
    console_logger.log_event_as_critical("This works !!! ")
    console_logger.log_event_as_error("This works !!! ")

    # cfg = Config(WRITE_FILES_TO_CLOUD=False)
    


    # Load webpage 
    # webpage_loader = WebPageLoader()
    # webpage_loader.load_page(football_url)
    

    # Close popup boxes if they appear on webpage
    # popup_handler = PopUpHandler()
    # popup_handler.close_popup()

    # # Extract data 
    # data_extractor = PremierLeagueTableScraper()
    # prem_league_scraped_content = data_extractor.scrape(football_url)
    

    # # Transform data 
    # data_transformer = PremierLeagueTableStandingsDataTransformer()
    # data_transformer.transform_data()


    # # Load data to machine 
    # if cfg.WRITE_FILES_TO_CLOUD is not False:
    #     data_uploader = LocalCSVPremierLeagueTableStandingsUploader()
    #     data_uploader.upload_file()

    