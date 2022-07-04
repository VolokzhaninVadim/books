# For work with tests
import unittest

# For work with parametrs of project
from src.Params import Params

# For work with data base
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import (
    create_engine, select
    )
from src.ORM import VacancyHTML

Base = declarative_base()
params = Params()


class DataAccessLayer:
    def __init__(self) -> None:
        '''
        Connect to test data base.
        '''
        self.conn_string = params.test_connection
        self.engine = create_engine(self.conn_string)
        self.connection = self.engine.connect()

    def create_objects(self) -> None:
        '''
        Create data base objects.
        '''
        VacancyHTML.__table__.create(bind=self.engine, checkfirst=True)


dal = DataAccessLayer()


def select_vacancy_html() -> list:
    '''
    Select data from vacancy_html.

    Returns
    -------
    list
        Data from vacancy_html.
    '''
    records = params.test_records_vacancy_html
    stmt1 = VacancyHTML.__table__.insert(records)
    dal.connection.execute(stmt1)
    stmt2 = select([VacancyHTML.vacancy_id, VacancyHTML.html])
    result = dal.connection.execute(stmt2)
    return result.fetchall()


class Test(unittest.TestCase):
    @classmethod
    def SetUpClass(cls):
        dal.conn_string = params.test_connection
        dal.connect()

    def test(self) -> None:
        '''
        Test select_vacancy_html.
        '''
        result = select_vacancy_html()
        self.assertEqual(result, [tuple(params.test_records_vacancy_html[0].values())])


