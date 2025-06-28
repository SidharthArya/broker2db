from sqlalchemy import create_engine
from functools import lru_cache

@lru_cache
def get_engine(url, database_name):
    connection_string = f'{url}/{database_name}'
    return create_engine(connection_string, echo=True, pool_pre_ping=True, pool_size=1, max_overflow=0)
