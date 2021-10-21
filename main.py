from datetime import datetime

from googleapiclient.discovery import Resource
from sqlalchemy import Table, MetaData, Column, DateTime, String
from sqlalchemy.engine import Engine

from sheets import get_sheets_data, get_sheets_service
from pg import get_engine, load


def parse(results: list) -> list:
    """Parse values from Sheets API

    Args:
        results (list): Values from Sheets API

    Returns:
        list: List of key-values
    """

    return [{key: value for key, value in zip(results[0], row)} for row in results[1:]]


def transform(rows: list) -> list:
    """Pipeline specific transform function

    Args:
        rows (list): List of key-values

    Returns:
        list: List of key-values
    """

    return [
        {
            "dt": row["dt"],
            "phone": row["phone"],
            "name": row["name"],
            "date_utc": datetime.strptime(row["Date_UTC"], "%m/%d/%Y").isoformat(
                timespec="seconds"
            ),
        }
        for row in rows
    ]


def run(service: Resource, engine: Engine, pipeline: dict) -> bool:
    """Run pipelines

    Args:
        service (Resource): Sheets API Service
        engine (Engine): sqlalchemy Engine
        pipeline (dict): Pipeline Definition

    Returns:
        bool: Worked or not
    """

    results = get_sheets_data(
        service,
        pipeline["spreadsheet_id"],
        pipeline["range"],
    )
    rows = pipeline['transform'](parse(results))
    return load(engine, pipeline["model"], rows)


def main():
    mock_pipelines = {
        "spreadsheet_id": "1DghH0_bOFq6Y9lhE_d349xZHw_9WOMUNLhVfwnhA-5E",
        "range": "'BQ C2L'!A:M",
        "transform": transform,
        "model": Table(
            "mock_pipelines",
            MetaData(schema="C2Leads"),
            Column("dt", DateTime(timezone=True)),
            Column("phone", String),
            Column("name", String),
            Column("date_utc", DateTime(timezone=True)),
        ),
    }
    return print(run(get_sheets_service(), get_engine(), mock_pipelines))


if __name__ == "__main__":
    main()
