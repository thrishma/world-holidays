# Create a list of countries from a given url and fetch 
# holidays for every country and save it per date

import requests
from bs4 import BeautifulSoup
import json
from s3_handler.s3Handler import create_file_in_s3

def create_json_files(file_name, content):
    # create new json files
    # with open(file_name, 'w') as outfile:
    #     json.dump(content, outfile)
    create_file_in_s3(file_name, content)
    

def find_countries():
    # Obtain a list of countries from the given website and save it.
    countries_names = {}
    holidays_page = requests.get("https://www.officeholidays.com/countries")
    holidays_page_soup = BeautifulSoup(holidays_page.content, 'html.parser')

    for li in holidays_page_soup.findAll('li', attrs={'style':';padding-bottom:10px;'}):
        name = li.find('a')
        cleaned_name = name.text.strip().lower()
        name_id = ''
        if (cleaned_name == 'bonaire, sint eustatius and saba'):
            name_id = 'bonaire-st-eustatius-saba'
        elif (cleaned_name == 'eswatini'):
            name_id = 'swaziland'
        elif (cleaned_name == 'north macedonia'):
            name_id = 'macedonia'
        elif (cleaned_name == 'united arab emirates'):
            name_id = 'uae'
        else:
            name_id = cleaned_name.replace(" ", "-").replace(",", "")

        countries_names[name_id] = name.text.strip()
    return countries_names


def find_holidays_per_country():
    # Obtain a list of holidays for each country and save them based on dates
    countries_names = find_countries()
    
    per_date_holidays_list = {}

    for country_id in countries_names.keys():
        print(f'Fetching details for {country_id}')
        per_country_holidays = requests.get(f'https://www.officeholidays.com/countries/{country_id}')
        per_country_holidays_soup = BeautifulSoup(per_country_holidays.content, 'html.parser')

        table = per_country_holidays_soup.findAll('table')
        tbody = table[0].find('tbody')

        for tr in tbody.findAll('tr'):
            td=tr.findAll('td')
            
            holiday_date = td[1].find('time', attrs={'itemprop':'startDate'})['datetime']
            holiday_related_data = {
                "day": td[0].text,
                "date": holiday_date,
                "holiday_name": td[2].find('a', attrs={'class':'country-listing'}).text,
                "comments": td[len(td)-1].text,
                "country_name": countries_names[country_id],
                "country_id": country_id
            }

            if holiday_date not in per_date_holidays_list:
                per_date_holidays_list[holiday_date] = [holiday_related_data]
            else:
                per_date_holidays_list[holiday_date].append(holiday_related_data)
    
    create_json_files('list_of_holidays_per_date.json', per_date_holidays_list)

def handler(event, context):
    try:
        find_holidays_per_country()
        return {
            "statusCode": 200, 
            "body": "www.officeholidays.com was successfully web scraped, and the holidays per date from around the world were stored!"
        }
    except Exception as e:
        return {
            "statusCode": 500, 
            "body": f"Unable to scarpe www.officeholidays.com, an error occured: {e}"
        }
        

    


