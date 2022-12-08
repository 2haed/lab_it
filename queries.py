import psycopg2
from db.credentials import con
from sql_commands import unique_planes, earliest_certificate, first_query

try:
    connection = psycopg2.connect(
        host=con['host'],
        user=con['user'],
        password=con['password'],
        database=con['database']
    )
    print("Connected successfully...", " ", sep='\n')
    with connection.cursor() as cursor:
        cursor.execute(first_query)
        for row in cursor:
            print(row)
except Exception as ex:
    print('Connection error...')
    print(ex)
