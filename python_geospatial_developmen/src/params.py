# For work with OS
import os
# For work with variables
from dotenv import load_dotenv
load_dotenv()

params = {
    'tor_host': os.environ.get('TOR_HOST'),
    'tor_control_port': os.environ.get('TOR_CONTROL_PORT'),
    'tor_socks_port': os.environ.get('TOR_SOCKS_PORT'),
    'tor_password': os.environ.get('TOR_PASSWORD'),
    'url_reformagkh_moscow_region': 'https://www.reformagkh.ru/opendata/export/184',
    'url_yandex_geocoder': 'https://yandex.ru/maps/213/moscow/search/{text_url}',
    'url_current_ip': 'https://api.ipify.org/?format=json',
    'url_osm': 'https://nominatim.openstreetmap.org/search?',
    'postgres_host':  os.environ.get('POSTGRES_HOST'),
    'postgres_port':  os.environ.get('POSTGRES_PORT'),
    'postgres_database':  os.environ.get('POSTGRES_DATABASE'),
    'postgres_login':  os.environ.get('POSTGRES_LOGIN'),
    'postgres_password':  os.environ.get('POSTGRES_PASSWORD')
}
