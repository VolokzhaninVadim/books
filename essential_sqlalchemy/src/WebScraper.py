# Work with params of project
from src.Params import Params

# Work with data base
from src.ORM import ORM
from src.ORM import Map, VacancyHTML

# Work with geo data
from src.Geo import Geo

# For work with HTTP queries
import requests
from requests import ConnectTimeout, ConnectionError, ReadTimeout
# For work with browser
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
# Work with HTML
from bs4 import BeautifulSoup
from urllib3.exceptions import MaxRetryError
# For generate fake user agent
from fake_useragent import UserAgent

# For work with TOR
from stem import Signal
from stem.control import Controller

# Work with json
import json

# Work with random objects
import random

# Work with parallelism
from multiprocessing.dummy import Pool as ThreadPool

# For work with date-time
from datetime import datetime
import time

# For work with spatial data
import shapely

# For monitoring cycle
from tqdm.notebook import tqdm as tqdm_notebook

# Create class object for change browser headers
ua = UserAgent(verify_ssl=False, cache=True)


class WebScraper:
    def __init__(self) -> None:
        self.params = Params()
        self.orm = ORM()
        self.geo = Geo()
# Set HTTP headers
        self.headers = {'User-Agent': ua.random}
# Get Chrome options
        chrome_options = Options()
# Set user-agent for browser
        chrome_options.add_argument(f'--user-agent={self.headers}')
# Without launch browser
        chrome_options.add_argument('--headless')
# Set HD window size
        chrome_options.add_argument("--window-size=1920x1080")
# Only for Linux for bypass the OS security model
        chrome_options.add_argument('--no-sandbox')
# Set proxies
        chrome_options.add_argument(f'--proxy-server={self.params.proxies}')
# Disable gpu
        chrome_options.add_argument('--disable-gpu')
# Not usage /dev/shm
        chrome_options.add_argument('--disable-dev-shm-usage')
# Get chrome driver
        self.driver = webdriver.Chrome(options=chrome_options)

    def close_all(self) -> None:
        '''
        Close the driver.
        '''
        for window_handle in self.driver.window_handles:
            self.driver.switch_to.window(window_handle)
            self.driver.close()
        self.driver.quit()

    def get_session(self) -> requests.sessions.Session:
        '''
        Get seesion.
        -------
        requests.sessions.Session
            return object of session requests.
        '''
        session = requests.session()
        session.proxies = {}
        session.proxies['http'] = self.params.proxies
        session.proxies['https'] = self.params.proxies
        return session

    def get_vacancies_list(self, vacancies_list: list) -> list:
        '''
        Get vacancies from list.

        Parameters
        ----------
        vacancies_list : list
        List of vacancies.

        Returns
        -------
        list
        List of vacancies for write into data base.
        '''
        result_list = []
        if vacancies_list:
            for vacancy in vacancies_list:
                if vacancy.get('compensation'):
                    compensation_from = vacancy.get('compensation').get('from')
                    compensation_to = vacancy.get('compensation').get('to')
                    compensation_currency_code = vacancy.get('compensation').get('currencyCode')
                else:
                    compensation_from, compensation_to, compensation_currency_code = None, None, None
                current_dict = {
                        'vacancy_id': vacancy.get('id'),
                        'vacancy_name': vacancy.get('name'),
                        'company_id': vacancy.get('company').get('id') if vacancy.get('company') else None,
                        'company_name': vacancy.get('company').get('name') if vacancy.get('company') else None,
                        'longitude':  vacancy.get('address').get('lat'),
                        'latitude': vacancy.get('address').get('lng'),
                        'compensation_from': compensation_from,
                        'compensation_to': compensation_to,
                        'compensation_currency_code': compensation_currency_code,
                        'date_load': datetime.utcnow()
                        }
                result_list.append(current_dict)
                return result_list
        else:
            return None

    def write_vacancies_map(self, rectangle: shapely.geometry.polygon.Polygon, url_param: str = None) -> None:
        '''
        Write vacancies into data base.

        Parameters
        ----------
        rectangle : shapely.geometry.polygon.Polygon
                Tuple of rectangle bound.
        url_param : str, optional
                url with data, by default None
        '''

        bottom_left_lng, bottom_left_lat, top_right_lng, top_right_lat = rectangle.bounds
        if url_param:
            url = url_param
        else:
            url = self.params.hh_map_vacancy_url.format(
                bottom_left_lng=bottom_left_lng,
                bottom_left_lat=bottom_left_lat,
                top_right_lng=top_right_lng,
                top_right_lat=top_right_lat
                )
        while True:
            time.sleep(10)
            self.change_ip()
            session = self.get_session()
            try:
                r = session.get(url, headers=self.headers, timeout=10)
                bsObj = BeautifulSoup(r.content, 'html5lib')
                if bsObj.text:
                    json_vacancies = json.loads(bsObj.text)
                    records = self.get_vacancies_list(json_vacancies['vacancies'])
                    if records:
                        self.orm.insert_values(records=records, table=Map)
                break
            except (ConnectTimeout, ConnectionError, ReadTimeout, MaxRetryError):
                continue

    def get_bypass_dict(
        self,
        first_number: int = 0,
        last_number: int = 152,
        multiplicity_number: int = 25
       ) -> dict:
        '''
        Generate bypass data.

        Parameters
        ----------
        first_number : int, optional
        First number, by default 0
        last_number : int, optional
        Last number, by default 152
        multiplicity_number : int, optional
        Multiple number, by default 25

        Returns
            -------
            dict
                Bypass data.
        '''

# Generate data for bypass
        start_numbers = []
        [start_numbers.append(i) for i in range(first_number, last_number, multiplicity_number)]
        last_numbers = []
        [last_numbers.append(i) for i in range(
                multiplicity_number, last_number + multiplicity_number,
                multiplicity_number)]
# Change last value
        last_numbers[len(last_numbers) - 1] = last_number

# Generate keys
        keys = [i for i in range(len(last_numbers))]
        bypass_dict = {k: [start_numbers[k], last_numbers[k]] for k in keys}
# Shuffle data
        return bypass_dict

    def write_mass_vacancies_map(self, threads_namber: int = 25) -> None:
        '''
        Mass write data.

        Parameters
        ----------
        threads_namber : int, optional
        Count of threads, by default 25
        '''

# Get all rectanles
        rectangle_all = self.geo.generate_rectangle()

# Shufle data
        random.shuffle(rectangle_all)
        bypass_dict = self.get_bypass_dict(multiplicity_number=threads_namber, last_number=len(rectangle_all))

        with ThreadPool(threads_namber) as p:
            for i in tqdm_notebook(bypass_dict.keys()):
                p.map(self.write_vacancies_map, rectangle_all[bypass_dict[i][0]:bypass_dict[i][1]])

    def test_proxy(self, timout: int = 10) -> bool:
        '''
        Testing proxy.

        Parameters
        ----------
        timout : int, optional
                Timout, by default 10

        Returns
        -------
        bool
                Result testing proxy.
        '''
        session1 = self.get_session()
        r1 = session1.get(self.params.url_get_external_ip, headers=self.headers, timeout=timout)

# Wait for change ip
        self.change_ip()
        time.sleep(timout)

        session2 = self.get_session()
        r2 = session2.get(self.params.url_get_external_ip, headers=self.headers, timeout=timout)
        return r1.content != r2.content

    def change_ip(self) -> None:
        '''
        Change IP via TOR.
        '''

        with Controller.from_port(address=self.params.tor_host, port=int(self.params.tor_control_port)) as controller:
            controller.authenticate(password=self.params.tor_password)
            controller.signal(Signal.NEWNYM)

    def write_vacancies_html(self, vacancy_id: int) -> None:
        '''
        Write vacancies html data base.

        Parameters
        ----------
        vacancy_id : int
                Id of vacancy.
        '''
        url = f'{self.params.hh_main_url}{vacancy_id}'
        while True:
            time.sleep(10)
            self.change_ip()
            session = self.get_session()
            try:
                r = session.get(url, headers=self.headers, timeout=10)
                bsObj = BeautifulSoup(r.content, 'html5lib')
                records = {
                        'vacancy_id': vacancy_id,
                        'html': str(bsObj),
                        'date_load': datetime.utcnow()
                }
                self.orm.insert_values(records=[records], table=VacancyHTML)
                break
            except (ConnectTimeout, ConnectionError, ReadTimeout, MaxRetryError):
                continue

    def write_mass_vacancies_html(self, threads_namber: int = 25) -> None:
        '''
        Mass write data.

        Parameters
        ----------
        threads_namber : int, optional
        Count of threads, by default 25
        '''

# Get id list
        id_list = self.orm.get_vacancy_id_map()
# Shufle data
        random.shuffle(id_list)
        bypass_dict = self.get_bypass_dict(multiplicity_number=threads_namber, last_number=len(id_list))

        with ThreadPool(threads_namber) as p:
            for i in tqdm_notebook(bypass_dict.keys()):
                p.map(self.write_vacancies_html, id_list[bypass_dict[i][0]:bypass_dict[i][1]])
