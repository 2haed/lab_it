query_create_tables = 'create table IF NOT EXISTS airports (index serial primary key, city text, document ' \
                      'text, company text, letter text); create table IF NOT EXISTS aircraft (index ' \
                      'serial primary key, type text, name text, id_mark text, serial_num text, ' \
                      'id_num text, registration_certificate text, date date); create table IF NOT EXISTS ' \
                      'airlines(index serial primary key, column_name int, company_short_name text, ' \
                      'company_name text, city text, planes text); create table IF NOT EXISTS ' \
                      'cargo_transportation(index serial primary key, city text, year int, ' \
                      'january decimal(8, 2), february decimal(8, 2), march decimal(8, 2), april decimal(' \
                      '8, 2), may decimal(8, 2), june decimal(8, 2), july decimal(8, 2), august decimal(' \
                      '8, 2), september decimal(8, 2), october decimal(8, 2), november decimal(8, 2), ' \
                      'december decimal(8, 2), january_to_december decimal(8, 2)); create table IF NOT ' \
                      'EXISTS passenger_transportation(index serial primary key, city text, year int, ' \
                      'january decimal(8, 2), february decimal(8, 2), march decimal(8, 2), april decimal(' \
                      '8, 2), may decimal(8, 2), june decimal(8, 2), july decimal(8, 2), august decimal(' \
                      '8, 2), september decimal(8, 2), october decimal(8, 2), november decimal(8, 2), ' \
                      'december decimal(8, 2), january_to_december decimal(8, 2)); '

unique_planes = 'select distinct type from aircraft'
earliest_certificate = 'select type, min(date) from aicraft group by type'
# first_query = 'select company from airports, cargo_trasportation, passenger_transportation'
