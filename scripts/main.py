import re
import time
import httpx
from bs4 import BeautifulSoup


def clean_data(response):
    '''cleans each weather station data into csv formatting'''

    # removes characters preceding 'yyyy'
    data = response.text.lower()
    index = data.index('yyyy')
    data = data[index:]

    # removes "provisional" substring
    data = data.replace(r'provisional', '')

    # replaces all consecutive whitespace except returns with single comma
    data_lines = data.splitlines()
    data_lines = [re.sub(r'[^\S\r\n]+', ',', line) for line in data_lines]

    # removes leading/trailing comma from lines where present
    data_lines = [line[1:] if line.startswith(',') else line for line in data_lines]
    # debugging trailing commas
    [print(f'\nLine: {repr(line[-10:-1])}') for line in data_lines]
    # data_lines = [line.replace('\r\n', '\n').replace(',\n', '\n') for line in data_lines]
    # data_lines = [line[:-2] + '\n' if line.startswith(',\n') else line for line in data_lines]

    # removes second line
    data = '\n'.join(data_lines[0:1] + data_lines[2:])

    return data


# requesting html from weather station page
page_url = (
    'https://www.metoffice.gov.uk/'
    'research/climate/maps-and-data/historic-station-data'
)
page_response = httpx.get(page_url)

# creating a soup object by parsing requested html
soup = BeautifulSoup(page_response, 'html.parser')

# finds table rows and data, gets first column (name), gets a tags and extracts 
# href url, gets data from url, updates dict with station name and data
table_rows = soup.find_all('tr')
station_dict = dict()

for index, row in enumerate(table_rows):
    # gets station name from table data
    column_0 = row.find('td')
    if column_0:
        station_name = column_0.text.lower().strip()
    # requests and stores station data from hyperlink in dictionary
    a_tags = row.find_all('a')
    for tag in a_tags:
        print(f'Requesting data from {station_name.title()}...')
        time.sleep(0.1)
        station_url = tag.get('href')
        station_response = httpx.get(station_url)
        station_dict[station_name] = clean_data(station_response)
    
    # writes aberporth data
    if index == 1:
        with open(f'{station_name}.txt', 'w') as file:
            file.write(station_dict[station_name])
        break