from datetime import datetime

from sheets import get_sheets_data, get_sheets_service


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
            "date_utc": datetime.strptime(row["Date_UTC"], '%m/%d/%Y').isoformat(
                timespec="seconds"
            ),
        }
        for row in rows
    ]


def main():
    """Main func"""

    results = get_sheets_data(
        get_sheets_service(),
        "1DghH0_bOFq6Y9lhE_d349xZHw_9WOMUNLhVfwnhA-5E",
        "'BQ C2L'!A:M",
    )
    rows = transform(parse(results))

if __name__ == "__main__":
    main()
