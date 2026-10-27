import os
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

def get_engine() -> Engine:
    """Create and return a SQLAlchemy engine for a SQLite database.

    The database file is created in the project root directory named ``etl_data.db``.
    """
    db_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'etl_data.db'))
    engine = create_engine(f'sqlite:///{db_path}', echo=False, future=True)
    return engine
