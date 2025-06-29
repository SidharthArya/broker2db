"""
Postgres Helper Functions
"""
from functools import lru_cache

from sqlalchemy import create_engine

@lru_cache
def get_engine(url, database_name):
    """
    Get engine for postgres
    """
    connection_string = f'{url}/{database_name}'
    return create_engine(connection_string,
                         echo=True,
                         pool_pre_ping=True,
                         pool_size=1,
                         max_overflow=0)
