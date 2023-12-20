import httpx
from selectolax.parser import HTMLParser
import pandas as pd
import numpy as np
from io import StringIO
from matplotlib import pyplot as plt
from matplotlib import colors as mcol
from matplotlib import rc
from matplotlib import cm


## Extracting data links
url = 'https://www.metoffice.gov.uk/research/climate/maps-and-data/historic-station-data'
headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:120.0) Gecko/20100101 Firefox/120.0'}

response = httpx.get(url, headers = headers)
html = HTMLParser(response.text)

links = []
for tag in html.tags('a'):
    attributes = tag.attributes
    if 'href' in attributes:
        links.append(attributes['href'])

data_links = []
for link in links:
    if '.txt' in link:
        data_links.append(link)


## Extracting data from data links
headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/119.0'}

responses = []
for link in data_links:
    responses.append(httpx.get(link))

data = []
for response in responses:
    data.append(response.text)


## Cleaning data

for table in data:
    table = (table
    .split('\n', 6)[6]
    .replace(' ', ',') 
    .replace(',,', ',')
    .replace(',,', ',')
    .replace(',,', ',')
    .replace('---', 'NaN')
    .replace('\r\n,', '\n')
    .replace(',\n', '\n')
    .replace('*', '')
)
print(len(data))