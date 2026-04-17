# from airflow.hooks.base import BaseHook


def get_pg_jdbc(conn_id: str) -> tuple:
    
    # conn = BaseHook.get_connection(conn_id)
    # url  = f'jdbc:postgresql://{conn.host}:{conn.port}/{conn.schema}'
    url = 'jdbc:postgresql://sources:5432/transport_db'
    props = {
        "user": "postgres",
        "password": "postgres",
        "driver": "org.postgresql.Driver"
    }
    return url, props
