import httpx
from bs4 import BeautifulSoup


# requesting html from weather station page
page_url = (
    'https://www.metoffice.gov.uk/'
    'research/climate/maps-and-data/historic-station-data'
)
page_response = httpx.get(page_url)

# creating a soup object by parsing requested html
soup = BeautifulSoup(page_response, 'html.parser')

# extracts all 'tr' (table rows), finds 'td' (table data), gets first 'td' 
# (station name) gets 'a' (anchor) tags and extracts href (contains url),
# updates dict with station name key and url value
table_rows = soup.find_all('tr')
station_dict = dict()

for row in table_rows:
    # gets station name from table data
    column_0 = row.find('td')
    if column_0:
        station_name = column_0.text.lower().strip()
    # requests and stores station data from hyperlink in dictionary
    a_tags = row.find_all('a')
    for tag in a_tags:
        print(f'Requesting data from {station_name.title()}...')
        station_url = tag.get('href')
        station_response = httpx.get(station_url)
        station_dict[station_name] = station_response.text
# prints second line of each dataset containing location and date
for name, data in station_dict.items():
    print(f'Station: {name}, Data: {data.splitlines()[1:2]}')