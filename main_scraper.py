
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

# ================================================ LOGGER ================================================


# Set up root root_logger 
root_logger     =   logging.getLogger(__name__)
root_logger.setLevel(logging.DEBUG)


# Set up formatter for logs 
file_handler_log_formatter      =   logging.Formatter('%(asctime)s  |  %(levelname)s  |  %(message)s  ')
console_handler_log_formatter   =   coloredlogs.ColoredFormatter(fmt    =   '%(message)s', level_styles=dict(
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
current_filepath    =   Path(__file__).stem
file_handler        =   logging.FileHandler('logs/scraper/' + current_filepath + '.log', mode='w')
file_handler.setFormatter(file_handler_log_formatter)


# Set up console handler object for writing event logs to console in real time (i.e. streams events to stderr)
console_handler     =   logging.StreamHandler()
console_handler.setFormatter(console_handler_log_formatter)


# Add the file handler 
root_logger.addHandler(file_handler)


# Only add the console handler if the script is running directly from this location 
if __name__=="__main__":
    root_logger.addHandler(console_handler)






# ================================================ CONFIG ================================================

# Load environment variables to session
load_dotenv()


# Add a flag for saving CSV files to the cloud 
WRITE_TO_CLOUD = False



# Set up environment variables

## AWS 

ACCESS_KEY                          =   os.getenv("ACCESS_KEY")
SECRET_ACCESS_KEY                   =   os.getenv("SECRET_ACCESS_KEY")
REGION_NAME                         =   os.getenv("REGION_NAME")
S3_BUCKET                           =   os.getenv("S3_BUCKET")
S3_FOLDER                           =   os.getenv("S3_FOLDER")


# Set up constants for S3 file to be imported
s3_client                                           =       boto3.client('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_ACCESS_KEY, region_name=REGION_NAME)



# Specify the constants for the scraper 
local_target_path               =   os.path.abspath('scraper/temp_storage/dirty_data')
match_dates                     =   ['2023-Apr-16']
# match_dates                     =   ['2022-Sep-01', '2022-Oct-01', '2022-Nov-01', '2022-Dec-01', '2023-Jan-01', '2023-Feb-01', '2023-Mar-01', '2023-Mar-07', '2023-Mar-08', '2023-Mar-12']
# match_dates                     =   ['2022-Sep-01', '2023-Mar-07']
table_counter                   =   0


# Set up the Selenium Chrome driver 
options = webdriver.ChromeOptions()
options.add_argument("--headless")

service = Service(executable_path=ChromeDriverManager().install())
chrome_driver = webdriver.Chrome(service=service, options=options)


# Assert the browsert is successfully running in a headless browser 
assert "--headless" in options.arguments, "WARNING: The Chrome driver is not rendering the script in a headless browser..."








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