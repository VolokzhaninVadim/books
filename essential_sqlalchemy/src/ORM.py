# Work with params of project
from src.Params import Params

# Work with datetime
from datetime import datetime

# Work with ORM и SQL
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.declarative.api import DeclarativeMeta
from sqlalchemy import (
    create_engine, distinct, select, func,
    Column, String, INTEGER, FLOAT,
    DateTime, TEXT,
    PrimaryKeyConstraint, inspect
    )

# Work with HTML
from bs4 import BeautifulSoup

# Work wih pandas table
import pandas as pd

# Base class
Base = declarative_base()


class Map(Base):
    '''
    Страницы с вакансиями.
    '''

    params = Params()
    __tablename__ = 'map'
    __table_args__ = (
        PrimaryKeyConstraint('vacancy_id', 'date_load'),
        {
            'comment': '''Карта вакансий'''
        }
    )
    vacancy_id = Column(
        'vacancy_id',
        INTEGER(),
        nullable=False,
        comment='Идентификатор вакансии'
        )
    vacancy_name = Column(
        'vacancy_name',
        String(),
        nullable=False,
        comment='Наименование вакансии'
        )
    company_id = Column(
        'company_id',
        INTEGER(),
        nullable=True,
        comment='Идентификатор компании'
        )
    company_name = Column(
        'company_name',
        String(),
        nullable=True,
        comment='Наименование компании'
        )
    longitude = Column(
        'longitude',
        FLOAT(),
        nullable=False,
        comment='Долгота ваканcии'
        )
    latitude = Column(
        'latitude',
        FLOAT(),
        nullable=False,
        comment='Широта ваканcии'
        )
    compensation_from = Column(
        'compensation_from',
        INTEGER(),
        nullable=True,
        comment='Зарплата от'
        )
    compensation_to = Column(
        'compensation_to',
        INTEGER(),
        nullable=True,
        comment='Зарплата до'
        )
    compensation_currency_code = Column(
        'compensation_currency_code',
        String(),
        nullable=True,
        comment='Зарплата код валюты'
        )
    date_load = Column(
        'date_load',
        DateTime(),
        nullable=False,
        default=datetime.utcnow(),
        comment='Дата и время вставки данных (UTC)'
        )


class VacancyHTML(Base):
    '''
    Вакансии с HTML.
    '''

    params = Params()
    __tablename__ = 'vacncy_html'
    __table_args__ = (
        PrimaryKeyConstraint('vacancy_id'),
        {
            'comment': '''Вакансии с HTML'''
        }
    )
    vacancy_id = Column(
        'vacancy_id',
        INTEGER(),
        nullable=False,
        comment='ID вакансии'
        )
    html = Column(
        'html',
        TEXT(),
        nullable=False,
        comment='Текст HTML'
        )
    date_load = Column(
        'date_load',
        DateTime(),
        nullable=False,
        default=datetime.utcnow(),
        comment='Дата и время вставки данных (UTC)'
        )


class Skill(Base):
    '''
    Навыки.
    '''

    params = Params()
    __tablename__ = 'skill'
    __table_args__ = (
        PrimaryKeyConstraint('skill', 'vacancy_id'),
        {
            'comment': '''Навыки'''
        }
    )
    vacancy_id = Column(
        'vacancy_id',
        INTEGER(),
        nullable=False,
        comment='ID вакансии'
        )
    skill = Column(
        'skill',
        String(),
        nullable=False,
        comment='Навыки'
        )
    date_load = Column(
        'date_load',
        DateTime(),
        nullable=False,
        default=datetime.utcnow(),
        comment='Дата и время вставки данных (UTC)'
        )


class ORM:
    def __init__(self):
        """
        Работа с ORM.
        """
        self.params = Params()
        self.engine = create_engine(self.params.string_connection, connect_args={'timeout': 100})
        self.inspector = inspect(self.engine)

    def create_delete_tables(
        self,
        table_list: list = [Map, VacancyHTML, Skill],
        delete: bool = False
    ) -> None:
        '''
        Create/delete tables.

        Parameters
        ----------
        table_list : list
            list of tables, by default [Page].
        delete : bool, optional
            Flag for delete/create tables, by default False.
        '''

# Create tables
        if delete:
            for table in table_list:
                table.__table__.drop(bind=self.engine, checkfirst=True)
        else:
            for table in table_list:
                table.__table__.create(bind=self.engine, checkfirst=True)

    def insert_values(
        self,
        records: list,
        table: DeclarativeMeta
    ):
        '''
         Insert values into table.

        Parameters
        ----------
        records : list
            list of values for insert into table.
        table : DeclarativeMeta
            Table for insert values.
        '''

        stmt = table.__table__.insert(records)
        with self.engine.connect().execution_options(autocommit=True) as conn:
            conn.execute(stmt)

    def get_vacancy_id_map(self) -> list:
        '''
        Get id from map table.

        Returns
        -------
        list
            List of id vacancies.
        '''

# Generate query
        stmt = select([distinct(Map.vacancy_id)]).select_from(Map)
# Get id
        id_list = []
        with self.engine.connect().execution_options(autocommit=True) as conn:
            result = conn.execute(stmt)
            for row in result:
                id_list.append(row[0])
        return id_list

    def insert_skill(self) -> None:
        '''
        insert skill to table.
        '''

        select_query = select([VacancyHTML.vacancy_id, VacancyHTML.html]).select_from(VacancyHTML)

        with self.engine.connect().execution_options(autocommit=True) as conn:
            result = conn.execute(select_query)
            for row in result:
                vacancy_id = row[0]
                html = row[1]
                bsobj = BeautifulSoup(html, 'html5lib')
                tags = bsobj.find_all('span', {'class': 'bloko-tag__section bloko-tag__section_text'})
                if tags:
                    tag_list = []
                    [tag_list.append(i.text) for i in tags]
                    records = []
                    for tag in tag_list:
                        record = {
                            'vacancy_id': vacancy_id,
                            'skill': tag
                        }
                        records.append(record)
                    insert_query = Skill.__table__.insert(records)
                    conn.execute(insert_query)

    def get_skill_df(self) -> pd.DataFrame:
        '''
        Get pandas data frame with skills and count vacansies.

        Returns
        -------
        pd.DataFrame
            Result of excecute query.
        '''

# Generate query
        stmt = (
            select([
                Skill.skill,
                (func.count(Skill.vacancy_id)).label('count_vacancy')
                ]).
            where(Skill.vacancy_id == Map.vacancy_id).
            group_by(Skill.skill).
            order_by(func.count(Skill.vacancy_id).desc()).
            limit(10)

        )

        return pd.read_sql(sql=stmt, con=self.engine)
