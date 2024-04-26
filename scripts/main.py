import time
import httpx
from bs4 import BeautifulSoup
from data_pipelines import DataProc


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
            print(f'Requesting data from {station_name.title()}...')
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