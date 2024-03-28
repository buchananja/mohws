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
        # print(
        #     f'lon: {geographic_dict[name][1]}, ' 
        #     f'lat: {geographic_dict[name][2]}, '
        #     f'amsl: {geographic_dict[name][0]}\n'
        # )
    # replaces all consecutive whitespace except returns with single comma
    data_lines = [re.sub(r'[^\S\r\n]+', ',', line) for line in data_lines]

    # removes leading/trailing comma from lines where present
    data_lines = [line[1:] if line.startswith(',') else line for line in data_lines]
    data_lines = [line[:-1] if line.endswith(',') else line for line in data_lines]

    # removes invalid data
    if ('site,closed') in data_lines[-1]:
        data_lines = data_lines[:-1]

    # adds geographical columns and station name
    print(data_lines[1])
    data_lines[1] += ',station_name,longitude,latitude,amsl'
    for station, location in geographic_dict.items():
        for i in range(1, len(data_lines)):
            data_lines[i] += f',{name}'
            data_lines[i] += f',{geographic_dict[station][1]}'
            data_lines[i] += f',{geographic_dict[station][2]}'
            data_lines[i] += f',{geographic_dict[station][0]}'

    # joins lines into string and removes unecessary text
    data = '\n'.join(data_lines)
    index_var = data.index('yyyy')
    data = data[index_var:]

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

        # writes data to file
        with open(f'data/{station_name}.csv', 'w') as file:
            file.write(station_dict[station_name])