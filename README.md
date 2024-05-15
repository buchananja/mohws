# mowhs (Met Office Historicla Weather Scraper)

This is a Python-based web scraper that extracts historical UK weather station data from the [Met Office website](https://www.metoffice.gov.uk/research/climate/maps-and-data/historic-station-data).

The aim of this project is to automate the collection, processing, formatting, and plotting of data across the UK's 37 historic weather stations to build a picture of how the UK's weather has changed over time and how it varies across the island.

<p align = "center">
  <img src = "station_image.png" alt = "image" height = "350">
</p>
<p align="center">
    <i><a href="https://en.wikipedia.org/wiki/Armagh_Observatory">Armagh Observatory</a> - example of typical UK weather station (<a href="https://www.geograph.ie/photo/5000777">Photo © Eric Jones (cc-by-sa/2.0)</a>)</i>
</p>

## Dependencies
**PIP**  
Create and activate a virtual environment then run following code to install all necessary dependencies:
```bash
pip install -r requirements/requirements.txt
```

**Conda**  
Run the following code to create a miniconda environment with all necessary dependencies installed:
```bash
conda env create -f requirements/environment.yml
```

## License
see [LICENSE.md](LICENSE.md)

## Contributing
see [CONTRIBUTING.md](CONTRIBUTING.md)
