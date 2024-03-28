import re
import time
import httpx
from bs4 import BeautifulSoup


def clean_data(response, name):
    '''cleans each weather station data into csv formatting'''

    data = response.text.lower()
    data_lines = data.splitlines()

    # gets longitude/latitude/asml
    if 'amsl' in data_lines[2]:
        lon_lat_line = f'{data_lines[1]}{data_lines[2]}'
    else:
        lon_lat_line = data_lines[1]
        location = lon_lat_line.split(', ')[0]
        lon_lat = lon_lat_line.split(', ')[1]
        amsl = lon_lat_line.split(', ')[2]
        print(f'{name}: location: {location}, lon_lat: {lon_lat}, amsl: {amsl}\n')

    # replaces all consecutive whitespace except returns with single comma
    data_lines = [re.sub(r'[^\S\r\n]+', ',', line) for line in data_lines]

    # removes leading/trailing comma from lines where present
    data_lines = [line[1:] if line.startswith(',') else line for line in data_lines]
    data_lines = [line[:-1] if line.endswith(',') else line for line in data_lines]

    # adds station_name column
    data_lines[0] += ',station_name'
    for i in range(1, len(data_lines)):
        data_lines[i] += f',{name}'

    # joins lines into string and removes unecessary text
    data = '\n'.join(data_lines)
    index_var = data.index('yyyy')
    data = data[index_var:]
    data = data.replace(r'provisional', '')

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
    # gets station name from table data and formats to lower snakecase
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
        station_dict[station_name] = clean_data(
            station_response,
            station_name
        )
        # writes data to file
        with open(f'data/{station_name}.csv', 'w') as file:
            file.write(station_dict[station_name])