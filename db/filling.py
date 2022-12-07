from db.credentials import con
import pandas as pd
import regex as re
import psycopg2
from sqlalchemy import create_engine

from sql_commands import query_create_tables


def airports() -> pd.DataFrame:
    df = pd.read_csv('../data/airports.csv', encoding='utf-8', header=None)
    df.rename(
        columns={df.columns[0]: 'city', df.columns[1]: 'document', df.columns[2]: 'company',
                 df.columns[3]: 'letter'}, inplace=True)
    df.dropna(inplace=True)
    df.city = df.city.apply(lambda row: ' '.join(
        filter(lambda x: x != 'км' and x not in re.findall(r"[-+]?(?:\d*\,\d+|\d+)", x) and len(x) > 2,
               row.split()[:3])))
    df.city = df.city.apply(lambda row: row.strip().title())
    return df


def aircraft() -> pd.DataFrame:
    df = pd.read_csv('../data/aircraft.csv', encoding='utf-8', on_bad_lines='skip', delimiter=';')
    df.rename(columns={df.columns[0]: 'type', df.columns[1]: 'name', df.columns[2]: 'id_mark',
                       df.columns[3]: 'serial_num', df.columns[4]: 'id_num',
                       df.columns[5]: 'registration_certificate', df.columns[6]: 'date'}, inplace=True)
    df.date = df.date.apply(lambda x: '-'.join(x.split('.')[::-1]))
    return df


def airlines() -> pd.DataFrame:
    df = pd.read_csv('../data/airlines.csv', header=None, encoding='utf-8')
    df.rename(columns={df.columns[0]: 'column_name', df.columns[1]: 'company_short_name',
                       df.columns[2]: 'company_name', df.columns[3]: 'city',
                       df.columns[4]: 'planes'}, inplace=True)
    df.planes = df.planes.str.split(',')
    df = df.explode('planes')
    df = df[df['planes'].notna()]
    df.planes = df.planes.apply(lambda row: row.strip())
    df.reset_index(inplace=True)
    return df.drop(['index'], axis=1).rename(columns={'level_0': 'index'})


def cargo_transportation() -> pd.DataFrame:
    df = pd.read_csv('../data/cargo transportation.csv', encoding='utf-8', on_bad_lines='skip',
                     delimiter=';')
    names = 'city', 'year', 'january', 'february', 'march', 'april', 'may', 'june', 'july', 'august', 'september', \
            'october', 'november', 'december', 'january_to_december'
    df.rename(columns={df.columns[i]: names[i] for i in range(15)}, inplace=True)
    return df


def passenger_transportation() -> pd.DataFrame:
    df = pd.read_csv('../data/passenger transportation.csv', encoding='utf-8',
                     on_bad_lines='skip',
                     delimiter=';')
    names = 'city', 'year', 'january', 'february', 'march', 'april', 'may', 'june', 'july', 'august', 'september', \
            'october', 'november', 'december', 'january_to_december'
    df.rename(columns={df.columns[i]: names[i] for i in range(15)},
              inplace=True)
    return df


def create_tables() -> None:
    try:
        connection = psycopg2.connect(
            host=con['host'],
            user=con['user'],
            password=con['password'],
            database=con['database']
        )
        print("Connected successfully...", " ", sep='\n')
        with connection.cursor() as cursor:
            cursor.execute(query_create_tables)
            connection.commit()
    except Exception as ex:
        print('Connection error...')
        print(ex)


def fill_values(airlines: pd.DataFrame, aircraft: pd.DataFrame, airports: pd.DataFrame,
                cargo_transportation: pd.DataFrame, passenger_transportation: pd.DataFrame) -> None:
    try:
        engine = create_engine(
            url="postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}".format(
                con['user'],
                con['password'],
                con["host"],
                con['port'],
                con['database']
            ))
        airports.to_sql("airports", engine, if_exists='replace')
        aircraft.to_sql('aircraft', engine, if_exists='replace')
        airlines.to_sql('airlines', engine, if_exists='replace')
        cargo_transportation.to_sql('cargo_transportation', engine, if_exists='replace')
        passenger_transportation.to_sql('passenger_transportation', engine, if_exists='replace')
        print('Values filled successfully')
    except Exception as ex:
        print(ex)


if __name__ == '__main__':
    create_tables()
    fill_values(airlines(), aircraft(), airports(), cargo_transportation(), passenger_transportation())
