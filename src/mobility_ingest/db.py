from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


def make_engine(url: str) -> Engine:
    return create_engine(url, pool_pre_ping=True)
