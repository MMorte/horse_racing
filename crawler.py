import pandas as pd
import requests
import re

from bs4 import BeautifulSoup
from typing import Union


class JockeyClubCrawler:
    def __init__(self, start_year: Union[int, str], end_year: Union[int, str]):
        table_urls = [
            f'http://www.dostihyjc.cz/vysledky.php?stat=1&amp;rok={year}' for year in range(start_year, end_year + 1)
        ]
        self.table_urls = table_urls


