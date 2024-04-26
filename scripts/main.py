import time
import httpx
import dpyp as dp
import pandas as pd
from io import StringIO
from bs4 import BeautifulSoup


class DropText:
    '''contains functionality for dropping unecessary text from station data'''
    
    
    @staticmethod
    def drop_site_closed_row(data_lines):
        '''removes invalid final row'''

        if ('site,closed') in data_lines[-1]:
            data_lines = data_lines[:-1]
        return data_lines
        

    @staticmethod
    def drop_units_row(data_lines):
        '''removes units row'''

        for index, line in enumerate(data_lines[:10]):
            if ('degc') in line:
                data_lines.pop(index)
        return data_lines


    @staticmethod
    def drop_header_text(data_lines):
        '''removes header text'''

        for index, line in enumerate(data_lines[:10]):
            if 'yyyy' in line:
                data_lines = data_lines[index:]
        return data_lines


class GetGeog:
    '''contains functionality for retriving geograpghical info for locations'''
    

    @staticmethod   
    def get_braemar_geography(geography_lines):
        '''extracts amsl/longitude/latitude from geography lines for braemar'''

        # extracts amsl
        amsl_1_phrase = dp.get_index_text(geography_lines, ',', 1)
        amsl_2_phrase = dp.get_index_text(geography_lines, ',', 4)
        amsl_1 = amsl_1_phrase[0:3]
        amsl_2 = amsl_2_phrase[0:3]

        # extracts year ranges from raw year phrases
        change_year_phrase = dp.get_text_between_indexes(amsl_2_phrase, '(', ')')
        change_year = dp.get_text_numerics(change_year_phrase, ' ', 1)[0]
        change_month = change_year_phrase.split(' ')[0].replace('(', '')
        change_month = dp.get_month_numeric(change_month)

        # extracts longitude/latitude for second year range
        lat_long_phrase = dp.get_index_text(geography_lines, ',', 3).strip().split(' ')
        latitude = lat_long_phrase[1]
        longitude = lat_long_phrase[3]

        # packs geography details into dict
        geography_dict = dict()
        geography_dict['change_year'] = change_year
        geography_dict['change_month'] = change_month
        geography_dict['amsl_1'] = amsl_1
        geography_dict['amsl_2'] = amsl_2
        geography_dict['longitude'] = longitude
        geography_dict['latitude'] = latitude

        return geography_dict


    @staticmethod
    def get_lowestoft_geography(geography_lines):
        '''
        extracts amsl/longitude/latitude from geography lines for 
        lowestoft_monckton_avenue
        '''

        first_location_phrase = geography_lines.split(' ')
        geography_dict = dict()
        geography_dict['change_year'] = first_location_phrase[7]
        geography_dict['amsl_1'] = dp.get_string_numerics(first_location_phrase[3])
        geography_dict['amsl_2'] = dp.get_string_numerics(first_location_phrase[18])
        geography_dict['longitude'] = first_location_phrase[17].strip(',')
        geography_dict['latitude'] = first_location_phrase[15]
        return geography_dict


    @staticmethod
    def get_nairn_geography(geography_lines):
        '''
        extracts amsl/longitude/latitude from geography lines for 
        nairn_druim
        '''

        first_location_phrase = geography_lines.split(' ')
        geography_dict = dict()
        geography_dict['change_year'] = first_location_phrase[2]
        geography_dict['amsl_1'] = dp.get_string_numerics(first_location_phrase[5])
        geography_dict['amsl_2'] = dp.get_string_numerics(first_location_phrase[16])
        geography_dict['longitude'] = first_location_phrase[15]
        geography_dict['latitude'] = first_location_phrase[13]
        return geography_dict


    @staticmethod
    def get_southampton_geography(geography_lines):
        '''
        extracts amsl/longitude/latitude from geography lines for 
        southampton_mayflower_park
        '''

        first_location_phrase = geography_lines.split(' ')
        geography_dict = dict()
        geography_dict['change_year'] = dp.get_string_numerics(first_location_phrase[8])
        geography_dict['amsl_1'] = dp.get_string_numerics(first_location_phrase[3])
        geography_dict['amsl_2'] = dp.get_string_numerics(first_location_phrase[16])
        geography_dict['longitude'] = first_location_phrase[15]
        geography_dict['latitude'] = first_location_phrase[13]
        return geography_dict


    @staticmethod
    def get_whitby_geography(geography_lines):
        '''
        extracts amsl/longitude/latitude from geography lines for whitby'''

        first_location_phrase = geography_lines.split(' ')
        geography_dict = dict()
        geography_dict['change_year'] = first_location_phrase[3]
        geography_dict['amsl_1'] = dp.get_string_numerics(first_location_phrase[6])
        geography_dict['amsl_2'] = dp.get_string_numerics(first_location_phrase[17])
        geography_dict['longitude'] = first_location_phrase[16]
        geography_dict['latitude'] = first_location_phrase[14]
        return geography_dict


class DataProcessing:
    '''contains data-pipelines for pre-processing and cleaning station data'''
    

    @staticmethod 
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
            (amsl, longitude, latitude) = ('---', '---', '---')
            geographic_dict[name] = (amsl, longitude, latitude)
            # geography_lines = ''.join(data_lines[1:3])
            # if name in 'braemar_no_2':
            #     GetGeog.get_braemar_geography(geography_lines)
            # if name in 'lowestoft_monckton_avenue':
            #     GetGeog.get_lowestoft_geography(geography_lines)
            # if name in 'nairn_druim':
            #     GetGeog.get_nairn_geography(geography_lines)
            # if name in 'southampton_mayflower_park':
            #     GetGeog.get_southampton_geography(geography_lines)
            # if name in 'whitby':
            #     GetGeog.get_whitby_geography(geography_lines)
        else:
            geography_line = data_lines[1]
            location = geography_line.split(', ')[1]
            longitude = location.split(' ')[3]
            latitude = location.split(' ')[1]
            amsl = geography_line.split(', ')[2]
            amsl = ''.join(filter(str.isdigit, amsl))

            # inserts longitude/latitude/amsl for station into dict
            geographic_dict[name] = (amsl, longitude, latitude)

        # formats output
        data_lines = [dp.replace_consecutive_spaces(line, ',') for line in data_lines]
        data_lines = [dp.remove_leading_char(line, ',') for line in data_lines]
        data_lines = [dp.remove_trailing_char(line, ',') for line in data_lines]
        data_lines = DropText.drop_site_closed_row(data_lines)
        data_lines = DropText.drop_header_text(data_lines)
        data_lines = DropText.drop_units_row(data_lines)

        # adds geographical and station name columns
        data_lines[0] += ',station_name,longitude,latitude,amsl'
        for station, location in geographic_dict.items():
            for index in range(1, len(data_lines)):

    # this section is broken, characters are not being correctly cleaned and are
    # appearing in the final csv file
                # removing unwanted characters from data
                data_lines[index] = (data_lines[index]
                    .replace('all,data,from,whitby', '')
                    .replace('change,to,monckton,ave', '')
                    .rstrip(',')
                )
                # geographic data input
                data_lines[index] += f',{name}'
                data_lines[index] += f',{geographic_dict[station][1]}'
                data_lines[index] += f',{geographic_dict[station][2]}'
                data_lines[index] += f',{geographic_dict[station][0]}'

                # removing unwanted characters from data
                data_lines[index] = (data_lines[index]
                    .replace('#', '')
                    .replace('*', '')
                    .replace('||', '')
                    .replace('$', '')
                    # .replace(',,', ',')
                    .replace('---', '')
                )
        return data_lines


    @staticmethod
    def pre_processing(csv_string):
        '''pipeline reads csv data as pandas dataframe and assigns data types'''

        df = pd.read_csv(StringIO(csv_string), sep = ',')
        
        rename_cols = {
            'yyyy': 'year',
            'mm': 'rainfall_mm',
            'tmax': 'max_temp',
            'tmin': 'min_temp',
            'mm': 'month',
            'af': 'air_frost_days',
            'sun': 'sun_duration',
            'amsl': 'height_above_sea_level'
        }
        float_cols = [
            'max_temp', 
            'min_temp', 
            'air_frost_days', 
            'rain',
            'sun_duration',
            'longitude',
            'latitude',
            'height_above_sea_level'
        ]
        string_cols = ['station_name']

        category_cols = ['year', 'month']

        df_processed = (df
            .pipe(dp.headers_rename, rename_cols)
            .pipe(dp.columns_to_string, string_cols)
            .pipe(dp.columns_to_float, float_cols)
            .pipe(dp.columns_optimise_numerics)
            .pipe(dp.columns_to_categorical, category_cols)
        )
        print(df_processed.info())


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

        # requests and stores station data from hyperlink into dictionary
        a_tags = row.find_all('a')
        for tag in a_tags:
            # print(f'Requesting data from {station_name.title()}...')
            time.sleep(0.1)
            station_url = tag.get('href')
            station_response = httpx.get(station_url)
            station_dict[station_name] = DataProcessing.clean_data(station_response, station_name)

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

    # calls pre-processing pipeline
    DataProcessing.pre_processing(combined_data)

if __name__ == '__main__':
    main()