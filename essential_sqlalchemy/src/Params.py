# Work with operating system
import os


class Params():
    '''
    Параметры проекта.
    '''
    def __init__(self):
        '''
            Инициализация объекта класса.
        '''
# Data base
        self.string_connection = 'sqlite:////root/jupyterlab/books/essential_sqlalchemy/hh.db'
# TOR
        self.tor_host = os.environ['TOR_HOST']
        self.tor_control_port = os.environ['TOR_CONTROL_PORT']
        self.tor_socks_port = os.environ['TOR_SOCKS_PORT']
        self.tor_password = os.environ['TOR_PASSWORD']
# Proxies
        self.proxies = f'socks5://{self.tor_host}:{self.tor_socks_port}'
        self.url_get_external_ip = 'https://ipinfo.io/ip'
# OSM
        self.osm_url = 'https://nominatim.openstreetmap.org/search?'
# Head hunter
        self.hh_map_vacancy_url = 'https://hh.ru/shards/vacancymap/searchvacancymap?area=1&clusters=false&enable_snippets=true&industry=7&items_on_page=100&label=with_address&no_magic=true&search_field=company_name&search_field=description&text=&bottom_left_lat={bottom_left_lat}&bottom_left_lng={bottom_left_lng}&top_right_lat={top_right_lat}&top_right_lng={top_right_lng}&width=1258&height=610.983'
        self.hh_main_url = 'https://hh.ru/vacancy/'
# Test
        self.test_connection = 'sqlite:///:memory:'
        self.test_records_vacancy_html = [{
                'vacancy_id': 0,
                'html': 'test'
                }]
