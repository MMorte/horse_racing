import pandas as pd
import requests
import re

from bs4 import BeautifulSoup
from decorators import allow_logging


class JockeyClub:
    """Crawl the horse race results on http://www.dostihyjc.cz/. 
    Extracts table results with headers and jockey cards information. 
    One year's races take about 3 minutes.
    """

    def __init__(self, start_year=1989, end_year=2019):
        # First race recorder dates back to 2. 4. 1989
        # Create a list of urls (of years) to scrape data from
        table_urls = [
            f"http://www.dostihyjc.cz/vysledky.php?stat=1&amp;rok={year}"
            for year in range(start_year, end_year + 1)
        ]
        self.table_urls = table_urls

    def _get_race_urls(self, url: str) -> list:
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

    def _get_result_headers(self, url: str) -> list:
        """Read headers of tables (one table = one race) to extract additional data/columns/features.

        Args:
            url (str): Race (day url) url from _get_race_urls

        Returns:
            list: list containing raw text data from headers
        """
        r = requests.get(url).text
        soup = BeautifulSoup(r, "html.parser")
        ## extracting extra features, cant get them elsewhere
        # Title is parsed here
        # there's only one title per race_url, carrying it all the way to the crawl_races procedure is ineffective
        # ergo i didnt figure out a better way
        title = soup.find("div", {"class": "text8"}).string
        self._race_date = re.search(r"\d+\.\d+\.\d+", title).group()
        self._race_city = title.split(" - ")[-1]
        # parse header information
        header_contents = soup.find_all("div", {"class": "hlavicka_dostihu"})
        headers = [header_content.contents for header_content in header_contents]
        return headers

    def _get_race_results(self, url: str) -> list:
        """Just a simple function to stay consistent with the _get_result_headers logic etc. 
        
        Args:
            url (str): race_url
        
        Returns:
            list: list of pd.DataFrames
        """
        # results are just the following tables
        tables = pd.read_html(url, encoding="utf-8", decimal=",")
        tables = tables[4:-2]
        return tables

    def _get_horse_urls(self) -> list:
        """Crawls horse statistics and extracts a list of urls with tables containing their handicaps on various dates (races).
        
        Returns:
            list: urls of horse handicaps (strings) such as - 'http://www.dostihyjc.cz/handicap_det.php?idkun=89787'
        """
        # base url containing data about horses including their handicaps
        url = "http://dostihyjc.cz/statistiky.php?stranka=22"

        # payloads to submit as data in requests.post
        # needed to access values behing dropdowns
        payloads = [
            ## includes all horses with races on the site
            # Rovina (not hurdle) race, horses of age 3
            {
                "x_stranka": 22,
                "s_rok": 2020,
                "s_typ": 1,
                "s_vek": 3,
                "obdobi": 1,
                "chgorder": "zobrazit",
            },
            # same as above but horses age 4 and above
            {
                "x_stranka": 22,
                "s_rok": 2020,
                "s_typ": 1,
                "s_vek": 4,
                "obdobi": 1,
                "chgorder": "zobrazit",
            },
            # hurdle/steeple/... horses aged 4 and above
            {
                "x_stranka": 22,
                "s_rok": 2020,
                "s_typ": 2,
                "s_vek": 4,
                "obdobi": 1,
                "chgorder": "zobrazit",
            },
        ]
        # list to append collected horse ids to
        horse_ids = []
        # loop over horse age/race type variants
        for payload in payloads:
            r = requests.post(url, data=payload)
            soup = BeautifulSoup(r.text, "html.parser")
            # ids are under a onclick parameter in tags: td#ht > span#text67
            # use regex to extract id from url
            td_tags = soup.find_all("td", {"class": "ht"})
            for td_tag in td_tags:
                span_tags = td_tag.find_all("span", {"class": "text67"})
                for span_tag in span_tags:
                    horse_url = span_tag["onclick"]
                    horse_id = re.search(r"idkun=(\d+)", horse_url).group(1)
                    horse_ids.append(horse_id)
        horse_urls = [
            "http://www.dostihyjc.cz/handicap_det.php?idkun=" + _id for _id in horse_ids
        ]
        return horse_urls

    def _preprocess_result_header(self, head: list) -> dict:
        """Extract features in a suitable format using regex and basic python. 

        Args:
            head (list): a single item from table_heads (_get_result_headers)
        
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

    def _preprocess_race_result(self, table: pd.DataFrame, head: dict) -> pd.DataFrame:
        """Renames and reshapes the race horse results table a bit, appends features extracted from headers
        
        Args:
            table (pd.DataFrame): table from read_tables
            head (dict): head from read_heads preprocessed by preprocess_race_header
        
        Returns:
            pd.DataFrame: returns formatted dataframe with features
        """
        # remove row containing column names
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
            "win_odds",
        ]
        table.columns = cols
        # create new_features
        table = table.assign(race_date=self._race_date)
        table = table.assign(race_city=self._race_city)
        for col_name, col_value in head.items():
            table.loc[:, col_name] = col_value
        return table

    @allow_logging
    def crawl_races(self) -> pd.DataFrame:
        """Crawl horse race data.
        
        Returns:
            pd.DataFrame: final formatted pandas dataframe with all items in tables and new features
        """
        ## 1. Create final df
        df = pd.DataFrame()
        ## 2. append tables to final df
        for table_url in self.table_urls:
            # Get all races in given year
            race_urls = self._get_race_urls(table_url)
            for race_url in race_urls:
                ## GET TABLES
                tables = self._get_race_results(race_url)
                headers = self._get_result_headers(race_url)
                ## PREP TABLES
                for table, header in zip(tables, headers):
                    header = self._preprocess_result_header(header)
                    table = self._preprocess_race_result(table, header)
                    df = pd.concat([df, table])
        return df

    @allow_logging
    def crawl_handicaps(self) -> pd.DataFrame:
        """Crawl horse tables to extract their handicap in individual races.
        
        Returns:
            pd.DataFrame: dataframe containing all horses and their handicaps
        """
        # output dataframe
        horse_handicaps = pd.DataFrame()
        horse_urls = self._get_horse_urls()
        for horse_url in horse_urls:
            horse_handicap = pd.read_html(horse_url, encoding="utf-8", decimal=",")[0]
            horse_handicap.columns = [
                "date",
                "handicap",
                "race_name",
                "race_type",
                "finish_order",
            ]
            # .iloc[0, 0] => first row is the horses name
            horse_handicap = horse_handicap.assign(horse_name=horse_handicap.iloc[0, 0])
            # second row are column names so we get rid of them
            horse_handicap = horse_handicap.loc[2:, :]
            horse_handicaps = pd.concat([horse_handicaps, horse_handicap])
        return horse_handicaps

