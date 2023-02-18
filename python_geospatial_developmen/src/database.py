# For work with parameters
from .params import params

# Work with database and SQL
from sqlalchemy.engine import URL
from sqlalchemy import create_engine, select, Table, Column, MetaData, Integer, DateTime, Text, Float, JSON
from sqlalchemy.engine.base import Engine
from sqlalchemy.sql.expression import Executable
from geoalchemy2 import Geometry
# For work with date
from datetime import datetime
# For work with data type
from typing import Union, List
# Fro work with data frame
from pandas import DataFrame


table_house = Table(
    'house',
    MetaData(schema='geo'),
    Column(
        'houseguid',
        Text,
        comment='Идентификатор дома'
    ),
    Column(
        'address',
        Text,
        comment='Адрес'
    ),
    Column(
        'living_rooms_amount',
        Integer,
        comment='Количество жилых квартир'
    ),
    Column('load_dttm', DateTime, default=datetime.utcnow, comment='Дата и время вставки данных (UTC)'),
    comment='Дома г. Москва'
)


table_address = Table(
    'address',
    MetaData(schema='geo'),
    Column(
        'address',
        Text,
        comment='Адрес'
    ),
    Column(
        'latitude',
        # Geometry('POINT'),
        Float,
        comment='Широта адреса'
    ),
    Column(
        'longitude',
        # Geometry('POINT'),
        Float,
        comment='Долгота адреса'
    ),
    Column('load_dttm', DateTime, default=datetime.utcnow, comment='Дата и время вставки данных (UTC)'),
    comment='Координаты домов'
)

table_polygon = Table(
    'polygon',
    MetaData(schema='geo'),
    Column(
        'polygon',
        JSON,
        comment='Полигон'
    ),
    Column('load_dttm', DateTime, default=datetime.utcnow, comment='Дата и время вставки данных (UTC)'),
    comment='Координаты домов'
)


class Database():
    def __init__(self):
        self.params = params

    @property
    def engine(self) -> Engine:
            '''
            Create engine.

            Returns
            -------
            Engine
                Engine for sqlalchemy.
            '''
            engine = create_engine(
                URL.create(
                    username=self.params.get('postgres_login'),
                    password=self.params.get('postgres_password'),
                    host=self.params.get('postgres_host'),
                    port=self.params.get('postgres_port'),
                    database=self.params.get('postgres_database'),
                    drivername='postgresql'
                    )
            )
            return engine

    def execute(self, query: Union[str, Executable], return_result: bool = False) -> Union[None, List]:
        '''
        Excecute query.

        Parameters
        ----------
        query : Union[str, Executable]
            Query.

        Returns
        -------
        Union[None, List]
            Query columns and query result.
        '''
        with self.engine.connect() as con:
                rows = con.execute(query)
                columns = rows.keys()
        if return_result:
            return [columns, rows]

    def get_house_df(self) -> DataFrame:
        '''
        Get house from database.

        Returns
        -------
        DataFrame
            Result dataframe.
        '''
        query = select(['*']).select_from(table_house)
        columns, rows = self.execute(query, return_result=True)
        df = DataFrame(rows, columns=columns)
        return df

    def insert_table_address(self, values: tuple) -> None:
        '''
        Insert data to table_address.

        Parameters
        ----------
        values : tuple
            Values for insert,
        '''
        table_address.metadata.bind = self.engine
        stmt = table_address.insert(values)
        self.execute(stmt)

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
