import pandas as pd
import requests
import re

from bs4 import BeautifulSoup
from typing import Union


class JockeyClub:
    def __init__(self, start_year: Union[int, str], end_year: Union[int, str]):
        table_urls = [
            f"http://www.dostihyjc.cz/vysledky.php?stat=1&amp;rok={year}"
            for year in range(start_year, end_year + 1)
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
        soup = BeautifulSoup(resp, "html.parser")
        # construct individual racing day urls
        race_urls = soup.find_all("a", {"class": "button-left"}, href=True)
        race_urls = race_urls[8:]
        race_urls = [
            "http://www.dostihyjc.cz/" + race_url["href"] for race_url in race_urls
        ]
        return race_urls

    def _read_heads(self, url: str) -> list:
        """Read headers of tables (one table = one race) to extract additional data/columns/features.
        
        Args:
            url (str): Race (day url) url from _read_races
        
        Returns:
            list: list containing raw text data from headers
        """
        r = requests.get(url).text
        soup = BeautifulSoup(r, "html.parser")
        table_heads = soup.find_all("div", {"class": "hlavicka_dostihu"})
        table_heads = [table_head.contents for table_head in table_heads]
        return table_heads

    def _preprocess_head(self, head: list) -> dict:
        """Extract features in a suitable format using regex and basic python. 

        Args:
            head (list): a single item from table_heads (_read_heads)
        
        Returns:
            dict: parsed header to be used as new feature
        """
        race_intraday_order_raw = head[0].split("start")[0]
        race_intraday_order = re.search(r"\d+", race_intraday_order_raw).group()
        race_start = head[0].split("start")[1].strip()
        race_id = head[2].strip()
        race_name = head[3].text
        race_type = head[5].split("-")[0].strip()
        horse_age_limit = head[5].split("-")[1].split(",")[-1].strip()
        race_length_raw = head[5].split("-")[1].split(",")[0]
        race_length = re.search(r"\d+", race_length_raw).group()
        track_quality_raw = head[-1]
        if "Stav dráhy" in track_quality_raw:
            track_quality = re.search(r"\d\.\d", track_quality_raw).group()
        else:
            track_quality = None
        parsed_head = {
            "race_intraday_order": race_intraday_order,
            "race_start": race_start,
            "race_id": race_id,
            "race_name": race_name,
            "race_type": race_type,
            "horse_age_limit": horse_age_limit,
            "race_length": race_length,
            "track_quality": track_quality,
        }
        return parsed_head

    def _read_tables(self, url: str) -> list:
        """Just a simple function to stay consistent with the _read_heads_ logic etc. 
        
        Args:
            url (str): race_url
        
        Returns:
            list: list of pd.DataFrames
        """
        # results are just the following tables
        tables = pd.read_html(url, encoding="utf-8")
        tables = tables[4:-2]
        return tables

    def _preprocess_table(self, table: pd.DataFrame, head: dict) -> pd.DataFrame:
        """[summary]
        
        Args:
            table (pd.DataFrame): table from read_tables
            head (dict): head from read_heads preprocessed by preprocess_head
        
        Returns:
            pd.DataFrame: returns formatted dataframe with features (no jockey information yet)
        """
        # remove column row
        table = table.loc[1:, :]
        # set column names for concat (cols before adding head)
        cols = [
            "finish_order",
            "horse_name",
            "weight",
            "jockey",
            "statement",
            "time",
            "starting_num",
            "trainer",
            "evq",
        ]
        table.columns = cols
        # create new_features
        for col_name, col_value in head.items():
            table.loc[:, col_name] = col_value
        return table

