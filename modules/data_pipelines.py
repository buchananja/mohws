import logging
import dpyp as dp
import pandas as pd
from io import StringIO
from .drop_text import DropText
from .get_geography import GetGeog


logger = logging.getLogger(__name__)


class DataProc:
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
            
            # gets geographical information for multi-line header locations
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
            # gets geographical information for simple header locations
            geography_line = data_lines[1]
            location = geography_line.split(', ')[1]
            longitude = location.split(' ')[3]
            latitude = location.split(' ')[1]
            amsl = geography_line.split(', ')[2]
            amsl = ''.join(filter(str.isdigit, amsl))

            # inserts longitude/latitude/amsl for station into dict
            geographic_dict[name] = (amsl, longitude, latitude)

        # formats output
        data_lines = [dp.RepText.replace_consecutive_whitespace(line, ',') for line in data_lines]
        data_lines = [dp.RemText.remove_leading_char(line, ',') for line in data_lines]
        data_lines = [dp.RemText.remove_trailing_char(line, ',') for line in data_lines]
        data_lines = DropText.drop_site_closed_row(data_lines)
        data_lines = DropText.drop_header_text(data_lines)
        data_lines = DropText.drop_units_row(data_lines)

        # adds geographical and station name columns
        data_lines[0] += ',station_name,longitude,latitude,amsl'
        for station, location in geographic_dict.items():
            for index in range(1, len(data_lines)):

    # characters not being correctly cleaned and appearing in final csv file
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
                    .replace('---', '')
                )
        return data_lines


    @staticmethod
    def pre_processing(csv_string):
        '''pipeline reads csv data as pandas dataframe and assigns dtypes'''


        df = pd.read_csv(
            StringIO(csv_string), 
            sep = ','
        )
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
            .pipe(dp.HeadClean.headers_rename, rename_cols)
            .pipe(dp.ColClean.columns_to_string, string_cols)
            .pipe(dp.ColClean.columns_to_float, float_cols)
            .pipe(dp.ColClean.columns_optimise_numerics)
            .pipe(dp.ColClean.columns_to_categorical, category_cols)
        )
        print(df_processed.info())