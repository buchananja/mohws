import logging
import dpyp as dp


# logger = logging.getLogger(__name__)


class GetGeog:
    '''contains functionality for retriving geograpghical info for locations'''
    

    @staticmethod   
    def get_braemar_geography(geography_lines):
        '''extracts amsl/longitude/latitude from geography lines for braemar'''

        # logger.info('Getting Braemar geography...')

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

        # logger.info('Getting Lowestoft geography...')

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

        # logger.info('Getting Nairn geography...')

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

        # logger.info('Getting Southampton geography...')

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

        # logger.info('Getting Whitby geography...')

        first_location_phrase = geography_lines.split(' ')
        geography_dict = dict()
        geography_dict['change_year'] = first_location_phrase[3]
        geography_dict['amsl_1'] = dp.get_string_numerics(first_location_phrase[6])
        geography_dict['amsl_2'] = dp.get_string_numerics(first_location_phrase[17])
        geography_dict['longitude'] = first_location_phrase[16]
        geography_dict['latitude'] = first_location_phrase[14]
        return geography_dict