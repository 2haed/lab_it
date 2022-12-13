import warnings
import numpy as np
from tabulate import tabulate
from db.credentials import con
import pandas as pd
import regex as re
from sqlalchemy import create_engine

warnings.simplefilter(action='ignore', category=FutureWarning)


def airports() -> pd.DataFrame:
    df = pd.read_csv('../data/airports.csv', encoding='utf-8', header=None)
    df.rename(
        columns={df.columns[0]: 'airport_name', df.columns[1]: 'document', df.columns[2]: 'company',
                 df.columns[3]: 'letter'}, inplace=True)
    df.dropna(inplace=True)
    df.airport_name = df.airport_name.apply(lambda row: ' '.join(
        filter(lambda x: x != 'км' and x not in re.findall(r"[-+]?(?:\d*\,\d+|\d+)", x) and len(x) > 2,
               row.split()[:3])))
    df.airport_name = df.airport_name.apply(lambda row: row.strip().title())
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
                       df.columns[2]: 'company_name', df.columns[3]: 'airport_name',
                       df.columns[4]: 'planes'}, inplace=True)
    df.planes = df.planes.str.split(',')
    df = df.explode('planes')
    df = df[df['planes'].notna()]
    df.planes = df.planes.apply(lambda row: row.strip())
    df.reset_index(inplace=True)
    df.planes = df.planes.str.split('  ')
    planes = pd.DataFrame(df["planes"].to_list(), columns=['plane', 'quantity'])
    planes.quantity = planes.quantity.apply(lambda row: row.lstrip('(').rstrip(')'))
    df.drop('planes', inplace=True, axis=1)
    df = pd.concat([df, planes], axis=1)
    return df.drop(['index'], axis=1).rename(columns={'level_0': 'index'})


def cargo_transportation() -> pd.DataFrame:
    df = pd.read_csv('../data/cargo transportation.csv', encoding='utf-8', on_bad_lines='skip',
                     delimiter=';', decimal='.', thousands='.')
    names = 'airport_name', 'year', 'january', 'february', 'march', 'april', 'may', 'june', 'july', 'august', 'september', \
            'october', 'november', 'december', 'january_to_december'
    df.rename(columns={df.columns[i]: names[i] for i in range(15)}, inplace=True)
    df.replace('***', 0, inplace=True)
    df.fillna(0, inplace=True)
    df.iloc[:, 2:] = df.iloc[:, 2:].apply(lambda x: x.str.replace(' ', ''))
    df.iloc[:, 2:] = df.iloc[:, 2:].apply(lambda x: x.str.replace(',', '.')).astype(
        float)
    df.year = df.year.apply(lambda x: x.replace(' ', '')).astype(int)
    return df


def passenger_transportation() -> pd.DataFrame:
    df = pd.read_csv('../data/passenger transportation.csv', encoding='utf-8',
                     on_bad_lines='skip',
                     delimiter=';', decimal='.', thousands='.')
    names = 'airport_name', 'year', 'january', 'february', 'march', 'april', 'may', 'june', 'july', 'august', 'september', \
            'october', 'november', 'december', 'january_to_december'
    df.rename(columns={df.columns[i]: names[i] for i in range(15)},
              inplace=True)
    df.replace('***', 0, inplace=True)
    df.fillna(0, inplace=True)
    df.iloc[:, 2:] = df.iloc[:, 2:].apply(lambda x: x.str.replace(' ', ''))
    df.iloc[:, 2:] = df.iloc[:, 2:].apply(lambda x: x.str.replace(',', '.')).astype(
        float)
    return df


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


def third_query(cargo_transportation: pd.DataFrame, passenger_transportation: pd.DataFrame,
                airports: pd.DataFrame) -> pd.DataFrame:
    cargo_traffic_for_2018 = pd.DataFrame()
    cargo_traffic_for_2018['airport_name'] = cargo_transportation[cargo_transportation['year'] == 2018][
        'airport_name']
    cargo_traffic_for_2018['cargo_traffic'] = cargo_transportation[cargo_transportation['year'] == 2018].iloc[:,
                                              2:15].sum(
        axis=1)
    cargo_traffic_for_2018['passenger_traffic'] = passenger_transportation[
                                                      passenger_transportation['year'] == 2018].iloc[:,
                                                  2:15].sum(axis=1)
    df = pd.merge(cargo_traffic_for_2018, airports, on='airport_name', how='left').drop(
        ['document', 'company', 'letter'],
        axis=1)
    return df


def fourth_query(cargo_traffic_for_2018: pd.DataFrame) -> pd.DataFrame:
    pass_median = np.median(cargo_traffic_for_2018['passenger_traffic'].unique())
    print(pass_median)
    cargo_median = np.median(cargo_traffic_for_2018['cargo_traffic'].unique())
    fourth_query = cargo_traffic_for_2018.query('passenger_traffic < @pass_median &'
                                                'cargo_traffic > @cargo_median')
    return fourth_query


def fifth_query(airlines: pd.DataFrame) -> pd.DataFrame:
    avia_types = set(plane for plane in airlines['plane'])
    rus_types = avia_types.intersection(set(unique_types))
    airlines['found_planes'] = [' '.join(rus_types.intersection(lst)) for lst in
                                airlines['plane']]
    fifth_query = airlines[airlines['found_planes'].astype(bool)]['company_name']
    return fifth_query


def sixth_query(passenger_transportation: pd.DataFrame) -> pd.DataFrame:
    passenger_year = passenger_transportation[passenger_transportation['year'] == 2017]
    passenger_year = passenger_year.drop(['year', 'january_to_december'], axis=1)
    passenger_year = passenger_year.dropna(axis=0, thresh=7)
    month_sum = pd.DataFrame(passenger_year.sum(axis=1)).set_axis(['avg_pass_traffic'], axis=1)
    # month_sum['airport_name'] = passenger_year.idxmax(axis=1)
    return month_sum


def last_query(cargo_transportation: pd.DataFrame) -> pd.DataFrame:
    cargo_year = cargo_transportation[cargo_transportation['year'] == 2017]
    merged_cargo = pd.merge(cargo_year, airports).drop(labels=['document', 'company', 'year', 'letter'], axis=1)
    return merged_cargo.groupby('airport_name').median(numeric_only=True).dropna(axis=0, thresh=7)


def main():
    fill_values(airlines(), aircraft(), airports(), cargo_transportation(), passenger_transportation())


if __name__ == '__main__':
    aircraft = aircraft()
    airlines = airlines()
    airports = airports()
    cargo_transportation = cargo_transportation()
    passenger_transportation = passenger_transportation()

    unique_types = pd.unique(aircraft['type'])
    third_query = third_query(cargo_transportation, passenger_transportation, airports)
    fifth_query = fifth_query(airlines)
    sixth_query = sixth_query(passenger_transportation)
    last_query = last_query(cargo_transportation)

    print(f'\nСписок уникальных пассажиров:\n {unique_types}')
    print(f'\nТретий запрос:\n {third_query}')
    print(f'\nЧетвертый запрос:\n {fourth_query(third_query)}')
    print(f'\nШестой запрос:\n {sixth_query}')
    print(f'\nПоследний запрос:\n {last_query}')
