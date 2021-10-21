import os

from sqlalchemy import create_engine, insert, Table
from sqlalchemy.engine import URL, Engine


def get_engine() -> Engine:
    return create_engine(
        URL.create(
            drivername="postgresql+psycopg2",
            username=os.getenv("PG_UID"),
            password=os.getenv("PG_PWD"),
            host=os.getenv("PG_HOST"),
            database=os.getenv("PG_DB"),
        ),
        executemany_mode="values",
        executemany_values_page_size=1000,
    )


def load(engine: Engine, model: Table, rows: list) -> bool:
    """Truncate then Load

    Args:
        engine (Engine): sqlalchemy Engine
        model (Table): sqlalchemy Table
        rows (list): List of key-values

    Returns:
        bool: Worked or not
    """
        
    with engine.connect() as conn:
        model.create(bind=engine, checkfirst=True)
        conn.execute(f'TRUNCATE TABLE "{model.schema}"."{model.name}"')
        loads = conn.execute(insert(model), rows)
        return loads.is_insert
