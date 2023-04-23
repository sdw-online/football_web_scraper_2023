# Football Scraper  

## Introduction 🚀

This is a simple data pipeline designed to scrape football data from the web. The aim is to scrape different types of football data for the top 5 leagues in Europe such as:

* league standings - [x]
* top goal scorers - []
* top assists - []
* top passers - []
* most shots on target - [] 


This Python script scrapes from the [twtd.co.uk] website for a specified match date then transforms the scraped data and uploads it to a local CSV file.



## Dependencies

Some of the modules used in this script include:

- os
- datetime
- pandas
- dotenv
- selenium
- logging



## Code blocks

The script consists of the following code blocks:

- Logger
- Config
- Webpage Loader
- Popup Handler
- Data Extractor
- Data Transformer
- Data Loader


Each code block is described below.


## Logger 📝

This code block creates a logging system to log messages, warnings and errors to a file in the `log` folder 


## Config 

This instantiates the environment variables required to access, store and process the data during each stage of the workflow


## Webpage Loader 🌐

This interface loads the webpage specified in the URL provided using a Chrome driver provided by Selenium. 


## Popup Handler 

Any popup windows that appear in the browser is closed using the method in this class. 


## Data Extractor 

This scrapes the football data from the HTML elements of the webpage using XPath selectors and stores the data as a list of lists.


## Data Transformer 

The scraped data is transformed and read into a Pandas dataframe. Then a `match_date` column is added to the dataframe. 


## Data Loader 

The data is persisted to the target destination of choice (either locally or to the cloud), and can be saved in any available format - CSV is the only available right now but there will be flexibility to add other options like JSON, text, parquet etc 





## Lessons Learnt  📚






## Conclusion 🏁

