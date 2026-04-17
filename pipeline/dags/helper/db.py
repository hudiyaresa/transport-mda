import os

PG_HOST     = os.environ.get('PG_HOST',     'sources')
PG_PORT     = os.environ.get('PG_PORT',     '5432')
PG_DB       = os.environ.get('PG_DB',       'transport_db')
PG_USER     = os.environ.get('PG_USER',     'postgres')
PG_PASSWORD = os.environ.get('PG_PASSWORD', 'postgres')

PG_URL = f'jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}'
PG_PROPS = {
    'user':     PG_USER,
    'password': PG_PASSWORD,
    'driver':   'org.postgresql.Driver',
}
