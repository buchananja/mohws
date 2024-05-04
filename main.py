import os
import time
import httpx
import logging
import importlib
from bs4 import BeautifulSoup
from modules.data_pipelines import DataProc


# configures root logger
main_path = os.path.dirname(os.path.abspath(__file__))

logging.basicConfig(
    filename = os.path.join(main_path, 'logs', 'met_scrape.log'),
    filemode = 'a',
    format = '%(asctime)s | %(levelname)s | %(name)s | %(message)s',
    datefmt = '%y/%m/%d %H:%M:%S',
    encoding = 'utf-8',
    level = logging.DEBUG
)
# adds module loggers to root logger
module_loggers = [
    'modules.data_pipelines',
    'modules.get_geography'
]
for module in module_loggers:
    mod = importlib.import_module(module)
    logger = getattr(mod, 'logger')
    logging.getLogger().addHandler(logger)


def main():
    '''scrapes data from weather station table via urls'''
    
    
    # requesting html from weather station page
    page_url = (
        'https://www.metoffice.gov.uk/'
        'research/climate/maps-and-data/historic-station-data'
    )
    page_response = httpx.get(page_url)
    soup = BeautifulSoup(page_response, 'html.parser')

    # gets name and url for each station
    table_rows = soup.find_all('tr')
    station_dict = dict()

    for row in table_rows:
        # gets formatted station from table
        column_0 = row.find('td')
        if column_0:
            station_name = column_0.text.lower().strip().replace(' ', '_')

        # stores requested station data from url
        for tag in row.find_all('a'):
            logger.info(f'Requesting data from {station_name.title()}...')
            time.sleep(0.1)
            station_url = tag.get('href')
            station_response = httpx.get(station_url)
            station_dict[station_name] = DataProc.clean_data(
                station_response, 
                station_name
            )
            # writes combines station data
            combined_data = str()
            for (index, data_lines) in enumerate(station_dict.values()):
                if index > 0:
                    data_lines = data_lines[1:]
                    data_lines[0] = '\n' + data_lines[0] # review this part
                station_data = '\n'.join(data_lines)
                combined_data += station_data
                
            with open(f'data/combined_data.csv', 'w') as file:
                file.write(combined_data)

    # calls pre-processing pipeline
    DataProc.pre_processing(combined_data)
    

if __name__ == '__main__':
    main()