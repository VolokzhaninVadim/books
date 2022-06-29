# For work with spatial data
from shapely.geometry import Polygon, MultiPolygon

# Work with params of project
from src.Params import Params

# For work with HTML-queries
import requests
# For generate fake user agent
from fake_useragent import UserAgent
ua = UserAgent(verify_ssl=False, cache=True)


class Geo():
    '''
    Работа с геообъектами.
    '''
    def __init__(
        self,
        city='Москва'
    ):
        '''
        Init.

        Parameters
        ----------
        city : str, optional
            Name of city, by default 'Москва'
        '''
        self.params = Params()
        self.city = city
# In degree 111,11 кm, 325 metrs - width of rectangle, 180 metrs - heigth of rectangle.
        self.dlon = 0.325/111.11
        self.dlat = 0.180/111.11

    def get_city_polygon(self) -> tuple:
        '''
        Polygon of the city.

        Returns
        -------
        tuple
            Tuple of the city data.
        '''
        params = {'format': 'json', 'limit': '1', 'polygon_geojson': '10', 'city': self.city}
        city = requests.get(self.params.osm_url, params=params)

# Get min and max coordinates
        min_coordinates = city.json()[0]['boundingbox'][0::2]
        max_coordinates = city.json()[0]['boundingbox'][1::2]

# Get polygon
        polygon_rectangle = Polygon([
            (float(min_coordinates[1]), float(min_coordinates[0])),
            (float(min_coordinates[1]), float(max_coordinates[0])),
            (float(max_coordinates[1]), float(max_coordinates[0])),
            (float(max_coordinates[1]), float(min_coordinates[0]))
        ])
        return (city.json(), polygon_rectangle, min_coordinates, max_coordinates)

    def get_rectangle(self, longitude: float, latitude: float) -> Polygon:
        '''
        Get rectangle from start coordinates.

        Parameters
        ----------
        longitude : float
            Longitude.
        latitude : float
            Latitude.

        Returns
        -------
        Polygon
            Polygon of rectangle.
        '''

# Get rectangle
        polygon_rectangle = Polygon([
            (longitude, latitude),
            (longitude, latitude + self.dlat),
            (longitude + self.dlon, latitude + self.dlat),
            (longitude + self.dlon, latitude)
            ])
        return polygon_rectangle

    def generate_rectangle(self) -> list:
        '''
        Generation of polygons what will be fill polygon of the city.

        Returns
        -------
        list
            List with bounds of polygon.
        '''

# Get city data
        city_geojson, polygon_rectangle, min_coordinates, max_coordinates = self.get_city_polygon()
        city_multipolygon = MultiPolygon(
            [Polygon([tuple(j) for j in i[0]]) for i in city_geojson[0]['geojson']['coordinates']]
            )
        city_boundary = [
            list(coordinates) for coordinates in list(polygon_rectangle.boundary.coords)
            ]

        rectangle_all = []
        start_latitude = city_boundary[0][1] - self.dlat
# Making a horizontal pass
        for j in range(round((city_boundary[1][1] - city_boundary[0][1]) / self.dlat)):
            start_longitude = city_boundary[0][0]
# Adding offset to go up
            start_latitude = start_latitude + self.dlat
# Making a vertical pass
            for i in range(round((city_boundary[3][0] - city_boundary[0][0]) / self.dlon)):
                current_rectangle = self.get_rectangle(longitude=start_longitude, latitude=start_latitude)
                current_rectangle_list = [list(i) for i in list(current_rectangle.boundary.coords)]
                start_longitude = current_rectangle_list[3][0]
                start_latitude = current_rectangle_list[3][1]
                if current_rectangle.intersects(city_multipolygon):
                    rectangle_all.append(current_rectangle)
        return rectangle_all
