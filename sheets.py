import gspread
from datetime import datetime
from google.oauth2.service_account import Credentials
from config import SPREADSHEET_ID


SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


class SheetClient:
    def __init__(self):
        import os
        import json

        creds_info = json.loads(os.getenv("GOOGLE_CREDENTIALS"))
        creds = Credentials.from_service_account_info(creds_info, scopes=SCOPES)

        gc = gspread.authorize(creds)
        self.sh = gc.open_by_key(SPREADSHEET_ID)

    def get_sheet(self, name):
        return self.sh.worksheet(name)

    def get_rows(self, sheet):
        return sheet.get_all_values()

    def upsert_summary(self, sheet_name, period_label, row):
        ws = self.get_sheet(sheet_name)
        values = ws.get_all_values()

        if not values:
            ws.append_row([
                "period",
                "start",
                "end",
                "pos",
                "neg",
                "sug",
                "total",
                "keywords",
                "summary",
                "created_at",
            ])
            values = ws.get_all_values()

        header = values[0]
        rows = values[1:]

        for i, r in enumerate(rows, start=2):
            if r and r[0] == period_label:
                ws.update(f"A{i}:J{i}", [row])
                return

        ws.append_row(row)
