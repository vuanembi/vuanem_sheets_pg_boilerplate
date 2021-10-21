import google.auth
from googleapiclient.discovery import build, Resource


def get_sheets_service() -> Resource:
    credentials, _ = google.auth.default(
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets.readonly",
        ]
    )
    return build("sheets", "v4", credentials=credentials)


def get_sheets_data(service: Resource, spreadsheet_id: str, range_: str) -> list:
    results = (
        service.spreadsheets()
        .values()
        .get(
            spreadsheetId=spreadsheet_id,
            range=range_,
        )
        .execute()
    )
    return results.get("values", [])
