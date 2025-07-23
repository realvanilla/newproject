import json
import streamlit as st
import gspread
from google.oauth2 import service_account


def get_config_from_sheet(sheet_id: str = "1Ud0Jw6JtSs9yBcxWnEJwlBccbDrhNK-zMsg_kX7MQBY",
                          worksheet_name: str = "Websites"):

    # Setup credentials from Streamlit secrets
    credentials = service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"]
    )

    # Setup client
    scope = ["https://spreadsheets.google.com/feeds",
             "https://www.googleapis.com/auth/drive"]
    scoped_credentials = credentials.with_scopes(scope)
    client = gspread.authorize(scoped_credentials)

    # Get data from sheet
    sheet = client.open_by_key(sheet_id)
    worksheet = sheet.worksheet(worksheet_name)
    all_values = worksheet.get_all_values()    # Map to config format and filter
    websites = []
    for row in all_values[1:]:  # Skip header
        if row[0]:  # Only process rows with number
            website = {
                "number": int(row[0]) if row[0].isdigit() else row[0],# Column A
                "website": row[1] if len(row) > 1 else "",  # Column B
                "status": row[4].strip() if len(row) > 4 else "",  # Column E
                "monetization": row[5].strip() if len(row) > 5 else "",  # Column F
                "account": row[6].strip() if len(row) > 1 else "",  # Column G
                "suffix": row[7] if len(row) > 8 else ""  # Column H
            }
            # Filter: suffix not blank AND status = "LIVE"
            if (website.get('suffix') and website.get('suffix').strip() != ''
                    and website.get('status', '').strip().upper() == 'LIVE'):
                websites.append(website)

    return {"websites": websites}