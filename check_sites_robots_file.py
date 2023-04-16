import requests


# Set the constants 
source_url   = f'https://www.twtd.co.uk/robots.txt'
response = requests.get(source_url)


# Checking the robots.txt for the football website to verify if I am permitted to scrape the site
if response.status_code == 200:
    print()
    print(response.text)
else:
    print(f'Unable to retrieve the robots.txt for this site: {response.status_code} ')