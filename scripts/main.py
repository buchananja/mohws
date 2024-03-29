import re
import time
import httpx
from bs4 import BeautifulSoup


def clean_data(response, name):
    '''cleans each weather station data into csv formatting'''

    # prepares data for splitting
    data = response.text.lower()
    data = data.replace(r'provisional', '')
    data_lines = data.splitlines()

    # gets geographical details of each station
    # location details appear on different lines for each station (1 or 1-2)
    geographic_dict = dict()
    if 'amsl' in data_lines[2]:
        geography_line = f'{data_lines[1]}{data_lines[2]}'
    else:
        # gets geographic location, extracts longitude/latitude/amsl
        geography_line = data_lines[1]
        location = geography_line.split(', ')[1]
        longitude = location.split(' ')[3]
        latitude = location.split(' ')[1]
        amsl = geography_line.split(', ')[2]
        amsl = ''.join(filter(str.isdigit, amsl))

        # inserts longitude/latitude/amsl for station into dict
        geographic_dict[name] = (amsl, longitude, latitude)

    # replaces all consecutive whitespace except returns with single comma
    data_lines = [re.sub(r'[^\S\r\n]+', ',', line) for line in data_lines]

    # removes leading/trailing comma from lines where present
    data_lines = [line[1:] if line.startswith(',') else line for line in data_lines]
    data_lines = [line[:-1] if line.endswith(',') else line for line in data_lines]

    # removes invalid data
    if ('site,closed') in data_lines[-1]:
        data_lines = data_lines[:-1]
    # removes units line
    for index, line in enumerate(data_lines[:10]):
        if ('degc') in line:
            data_lines.pop(index)
    # removes header info
    for index, line in enumerate(data_lines[:10]):
        if 'yyyy' in line:
            data_lines = data_lines[index:]
    # adds geographical and station name columns
    data_lines[0] += ',station_name,longitude,latitude,amsl'
    for station, location in geographic_dict.items():
        for index in range(1, len(data_lines)):
            data_lines[index] += f',{name}'
            data_lines[index] += f',{geographic_dict[station][1]}'
            data_lines[index] += f',{geographic_dict[station][2]}'
            data_lines[index] += f',{geographic_dict[station][0]}'

    return data_lines


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

for row in table_rows:
    # gets formatted station from table
    column_0 = row.find('td')
    if column_0:
        station_name = column_0.text.lower().strip().replace(' ', '_')
    # requests and stores station data from hyperlink intof dictionary
    a_tags = row.find_all('a')
    for tag in a_tags:
        print(f'Requesting data from {station_name.title()}...')
        time.sleep(0.1)
        station_url = tag.get('href')
        station_response = httpx.get(station_url)
        station_dict[station_name] = clean_data(station_response, station_name)

        # combines all station data
        combined_data = str()
        for index, data_lines in enumerate(station_dict.values()):
            if index > 0:
                data_lines = data_lines[1:]
                data_lines[0] = '\n' + data_lines[0]
            station_data = '\n'.join(data_lines)
            combined_data += station_data
        # writes data to file
        with open(f'data/combined_data.csv', 'w') as file:
            file.write(combined_data)