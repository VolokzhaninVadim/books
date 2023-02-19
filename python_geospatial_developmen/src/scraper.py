# For work with parameters
from .params import params
# For work with database
from .database import Database, table_house, table_polygon

# For pulling data out of HTML
from bs4 import BeautifulSoup
# For work with data frame
import pandas as pd
# For HTML query
import requests
from requests import ConnectTimeout, ConnectionError, ReadTimeout
from requests.exceptions import ChunkedEncodingError
from requests.sessions import Session
import urllib
# For work wit tor
import stem
from stem.control import Controller
from stem.process import launch_tor_with_config
# For reqular expressions
import re
# For work with data type
from typing import List
# For work with time
import time
# For work with regular expression
import re
# For work with spatial data
from shapely.geometry import Polygon
# For work with json
import json
# For work with sql
from sqlalchemy import dialects
# Work with parallelism
from multiprocessing.dummy import Pool as ThreadPool
# For work with loop
from tqdm import tqdm


class Scraper():
    '''
    Scraper.
    '''

    def __init__(self):
        self.params = params
        self.database = Database()

    def prepare_text(self, raw_text: str) -> str:
        '''
        Prepare text.

        Parameters
        ----------
        raw_text : str
            Raw text.

        Returns
        -------
        str
            Prepare text
        '''
        patern1 = re.compile(r'/')
        result = patern1.sub('_', raw_text)
        patern2 = re.compile(r'\sк.')
        return patern2.sub(r' к', result)

    def change_session_ip(self) -> Session:
        '''
        Change session IP.

        Returns
        -------
        Session
            Session with changed IP.
        '''
        session = requests.session()
        with Controller.from_port(address=params.get('tor_host'), port=int(params.get('tor_control_port'))) as controller:
                controller.authenticate(password=params.get('tor_password'))
                controller.signal(stem.Signal.NEWNYM)
        session.proxies = {}
        proxies = 'socks5h://{tor_host}:{tor_socks_port}'.format(tor_host=params.get('tor_host'), tor_socks_port=params.get('tor_socks_port'))
        session.proxies['http'] = proxies
        session.proxies['https'] = proxies
        return session

    def check_ip(self, session: Session) -> dict:
        '''
        Check session IP.

        Parameters
        ----------
        session : Session
            Session

        Returns
        -------
        dict
            Current IP.
        '''
        result = session.get(params.get('url_current_ip'))
        return result.json()

    def yandex_geocoder(self, address_text: str, delay: int = 5) -> List[str]:
        '''
        Yandex geocoder.

        Parameters
        ----------
        address_text : str
            Address.
        delay : int
            Delay in seconds for load page.

        Returns
        -------
        List[str]
            [latitude, longitude]
        '''
        # Loop until we get the coordinates
        while True:
            try:
                text_url = urllib.parse.quote(f'{address_text}')
                url = self.params.get('url_yandex_geocoder').format(text_url=text_url)
                session = self.change_session_ip()
                r = session.get(url)
                time.sleep(delay)
                bsObj = BeautifulSoup(r.text, 'html5lib')
                if re.findall('нам очень жаль', bsObj.text.lower()):
                    continue
                if bsObj.find_all("div", class_="toponym-card-title-view__coords"):
                    coords_text = bsObj.find_all("div", class_="toponym-card-title-view__coords")[0].text
                    coords_raw = re.sub(r'Координаты:|\s', '', coords_text).split(',')
                    latitude, longitude = [float(i) for i in coords_raw]
                    break
                else:
                    latitude, longitude = [0.0, 0.0]
                    break
            except (ChunkedEncodingError, ConnectTimeout, ConnectionError, ReadTimeout, IndexError) as e:
                continue
        return [latitude, longitude]

    def get_moscow_houses_df(self) -> pd.DataFrame:
        '''
        Get Moscow houses.

        Returns
        -------
        pd.DataFrame
            Moscow houses.
        '''
        df = pd.read_csv(
            filepath_or_buffer=self.params.get('url_reformagkh_moscow_region'),
            chunksize=100,
            compression='zip',
            sep=';',
            low_memory=False
            ).read()
        columns = ['houseguid', 'address', 'living_rooms_amount']
        df = df[columns]
        return df[~df['houseguid'].isna()]

    def insert_house_df(self) -> None:
        df = self.get_moscow_houses_df()
        df.to_sql(
            name=table_house.name,
            schema=table_house.schema,
            con=self.database.engine,
            if_exists='replace',
            index=False
        )

    def insert_table_address(self, raw_text: str) -> None:
        '''
        Insert values to table_address.

        Parameters
        ----------
        raw_text : str
            Raw text.
        '''
        prepare_text = self.prepare_text(raw_text)
        latitude, longitude = self.yandex_geocoder(address_text=prepare_text)
        values = (raw_text, latitude, longitude)
        self.database.insert_table_address(values)

    def get_region_polygon(self, query: str = 'Москва Южный административный округ') -> tuple:
        '''
        Get polygon of the region.

        Parameters
        ----------
        city : str, optional
            Region name in query, by default 'Москва Южный административный округ'

        Returns
        -------
        tuple
            Tuple of the region data.
        '''
        params = {'format': 'json', 'limit': '1', 'polygon_geojson': '10', 'q': query}
        region = requests.get(self.params.get('url_osm'), params=params)

# Get min and max coordinates
        min_coordinates = region.json()[0]['boundingbox'][0::2]
        max_coordinates = region.json()[0]['boundingbox'][1::2]

# Get polygon
        polygon_rectangle = Polygon([
            (float(min_coordinates[1]), float(min_coordinates[0])),
            (float(min_coordinates[1]), float(max_coordinates[0])),
            (float(max_coordinates[1]), float(max_coordinates[0])),
            (float(max_coordinates[1]), float(min_coordinates[0]))
        ])
        return (region.json(), polygon_rectangle, min_coordinates, max_coordinates)

    def insert_table_polygon(self) -> None:
        '''
        Insert data to table_polygon.
        '''

        region_json, polygon_rectangle, min_coordinates, max_coordinates = self.get_region_polygon()
        pd.DataFrame(region_json)['geojson'].apply(json.dumps).to_sql(
            name=table_polygon.name,
            schema=table_polygon.schema,
            con=self.database.engine,
            if_exists='replace',
            index=False,
            dtype={"polygon":dialects.postgresql.JSONB}
        )

    # def mass_insert_table_address(self, multiplicity_number: int=25) -> None:
    #     '''
    #     Mass insert data to table_address.

    #     Parameters
    #     ----------
    #     multiplicity_number : int, optional
    #         Multiplicity number for generate bypass dict and
    #         set thread count, by default 25
    #     '''

    #     # Get house dataframe.
    #     df = self.database.get_house_df()

    #     # Get bypass dataframe
    #     bypass_dict = self.database.get_bypass_dict(
    #         last_number=df.shape[0],
    #         multiplicity_number=multiplicity_number
    #     )

    #     # Get data
    #     with ThreadPool(multiplicity_number) as p:
    #         for i in tqdm(bypass_dict.keys()):
    #             p.map(
    #                 self.insert_table_address,
    #                 df[bypass_dict[i][0]:bypass_dict[i][1]]['address']
    #             )