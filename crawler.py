import pandas as pd
import requests
import re

from bs4 import BeautifulSoup
from typing import Union


class JockeyClub:
    def __init__(self, start_year: Union[int, str], end_year: Union[int, str]):
        table_urls = [
            f'http://www.dostihyjc.cz/vysledky.php?stat=1&amp;rok={year}' for year in range(start_year, end_year + 1)
        ]
        self.table_urls = table_urls

    def _read_races(self, url: str) -> list:
        """Use requests with bs4 to return a list of urls of every race that happened in a given year."
        
        Args:
            url (str): a url from the table_urls (link for given year, from the class itself)
        
        Returns:
            list: usually 52 urls for each week with a horse race
        """
        resp = requests.get(url).text
        soup = BeautifulSoup(resp, 'html.parser')
        # construct individual racing day urls
        race_urls = soup.find_all('a', {'class':'button-left'}, href=True)
        race_urls = race_urls[8:]
        race_urls = ['http://www.dostihyjc.cz/' + race_url['href'] for race_url in race_urls]
        return race_urls


    def _read_heads(self, url: str) -> list:
        """Read headers of tables (one table = one race) to extract additional data/columns/features.
        
        Args:
            url (str): Race (day url) url from _read_races
        
        Returns:
            list: list containing raw text data from headers
        """
        r = requests.get(url).text
        soup = BeautifulSoup(r, 'html.parser')
        table_heads = soup.find_all('div', {'class':'hlavicka_dostihu'})
        table_heads = [table_head.contents for table_head in table_heads]
        return table_heads
