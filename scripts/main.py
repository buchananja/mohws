import re
import time
import httpx
from bs4 import BeautifulSoup


def get_index_text(phrase, split_char, index):
    '''gets text from index in phrase split by character'''

    text = phrase.strip().split(split_char)[index]

    return text


def get_text_numerics(phrase, split_char, *indexes):
    '''
    takes a phrase and list of indexes to extract numbers from; appends numbers
    to a list and returns
    '''

    if all([isinstance(index, int) for index in indexes]):      
        numbers = list()
        for index in indexes:
            try:
                number = ''.join(filter(str.isdigit, phrase.split(split_char)[index]))
                numbers.append(number)
            except IndexError:
                print('Index out of bounds')
    else:
        print('Index not intiger')
        raise ValueError
            
    return tuple(numbers)
    

def get_text_between_indexes(phrase, char_1, char_2):
    '''extracts text within a phrase between two characters'''

    start_index = phrase.index(char_1)
    end_index = phrase.index(char_2)
    extracted_text = phrase[start_index:end_index]

    return extracted_text


def get_braemar_geography(geography_lines):
    '''extracts amsl/longitude/latitude from geography lines for braemar'''

    # extracts amsl
    amsl_1_phrase = get_index_text(geography_lines, ',', 1)
    amsl_2_phrase = get_index_text(geography_lines, ',', 4)
    amsl_1 = amsl_1_phrase[0:3]
    amsl_2 = amsl_2_phrase[0:3]

    # extracts year ranges from raw year phrases
    first_year_phrase = get_text_between_indexes(amsl_1_phrase, '(', ')')
    first_year_range = get_text_numerics(first_year_phrase, ' ', 0, -1)
    second_year_phrase = get_text_between_indexes(amsl_2_phrase, '(', ')')
    second_year = get_text_numerics(second_year_phrase, ' ', 1)

    # extracts longitude/latitude for second year range
    lat_long_phrase = get_index_text(geography_lines, ',', 3).strip().split(' ')
    latitude = lat_long_phrase[1]
    longitude = lat_long_phrase[3]

    # packs geography details into dict
    geography_dict = dict()
    geography_dict[first_year_range] = (amsl_1, None, None)
    geography_dict[second_year] = (amsl_2, longitude, latitude)

    return geography_dict


def get_lowestoft_geography(geography_lines):
    '''
    extracts amsl/longitude/latitude from geography lines for 
    lowestoft_monckton_avenue
    '''

    # extracts amsl
    first_location_phrase = geography_lines[0].split(' ')
    print(first_location_phrase)
    # amsl_1_phrase = get_index_text(geography_lines, ',', 1)
    # amsl_2_phrase = get_index_text(geography_lines, ',', 4)
    # amsl_1 = amsl_1_phrase[0:3]
    # amsl_2 = amsl_2_phrase[0:3]


def clean_data(response, name):
    '''pipeline which formats wheather station data as csv'''

    # prepares data for splitting
    data = response.text.lower()
    data = data.replace(r'provisional', '')
    data_lines = data.splitlines()

    # gets geographical details of each station
    # location details appear on different lines for each station (1 or 1-2)
    geographic_dict = dict()
    if 'amsl' in data_lines[2]:
        geography_lines = ''.join(data_lines[1:3])
        # print(f'{name}: {geography_lines.split(',')}\n')
        
        if name in 'braemar_no_2':
            braemar_dict = get_braemar_geography(geography_lines)
        if name in 'lowestoft_monckton_avenue':
            lowestoft_dict = get_lowestoft_geography(geography_lines)
            # [print(key, value) for key, value in braemar_dict.items()]
    else:
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

    # removes invalid final row
    if ('site,closed') in data_lines[-1]:
        data_lines = data_lines[:-1]
    # removes units row
    for index, line in enumerate(data_lines[:10]):
        if ('degc') in line:
            data_lines.pop(index)
    # removes header text
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


def main():
    # requesting html from weather station page
    page_url = (
        'https://www.metoffice.gov.uk/'
        'research/climate/maps-and-data/historic-station-data'
    )
    page_response = httpx.get(page_url)

    # creating a soup object by parsing requested html
    soup = BeautifulSoup(page_response, 'html.parser')

    # finds table rows and data, gets first column (name), gets a tags and extracts 
    # href url, gets data from url, updates dict with station name and csv data 
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
            # print(f'Requesting data from {station_name.title()}...')
            time.sleep(0.1)
            station_url = tag.get('href')
            station_response = httpx.get(station_url)
            station_dict[station_name] = clean_data(station_response, station_name)

            # combines all station data
            combined_data = str()
            for (index, data_lines) in enumerate(station_dict.values()):
                if index > 0:
                    data_lines = data_lines[1:]
                    data_lines[0] = '\n' + data_lines[0] # review this part
                station_data = '\n'.join(data_lines)
                combined_data += station_data
            # writes data to file
            with open(f'data/combined_data.csv', 'w') as file:
                file.write(combined_data)

if __name__ == '__main__':
    main()