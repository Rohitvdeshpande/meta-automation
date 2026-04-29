# ============================================================
# META SPENDS REPORT SCRIPT (GITHUB READY VERSION)
# ============================================================

import requests
import pandas as pd
import time
import gspread
import os
import json
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta


# =========================
# CONFIG
# =========================
ACCESS_TOKEN = "EAAX6VtO5tO8BRGQjZCOmZA5UDfsHa2SzZAvbaLX9X6n36stZAyZA4PBcgZA0ZARdSgrcm6swHYyYq0SXwrZAZBStK3EJIUQLmddCNFvCB2ZA3nPy9Ao2n0hu4MpitzMoanDhXlGGQt1J738vFZBwls5fZCk7o2Q9tZBaPt5HO2dOQU7ZChBPiCNaK8eZBIIeZB8kFpQ4UpXE"

BASE_URL = "https://graph.facebook.com/v18.0"

INCREMENTAL_MODE = True

AD_ACCOUNTS = [
    "act_61747633",
    "act_1638356833183465",
    "act_375509061368472",
    "act_575017180766466"
]

FIELDS = [
    "date_start",
    "campaign_name",
    "adset_name",
    "spend",
    "impressions",
    "clicks",
    "cpm",
    "ctr",
    "reach"
]


# =========================
# SAFE REQUEST
# =========================
def safe_request(url, params=None, retries=3):

    for attempt in range(retries):
        try:
            res = requests.get(url, params=params, timeout=30).json()

            if "error" in res:
                print(f"⚠️ Attempt {attempt+1}:", res)
                time.sleep(5)
                continue

            return res

        except Exception:
            time.sleep(5)

    return None


# =========================
# FETCH INSIGHTS
# =========================
def fetch_insights(account_id):

    url = f"{BASE_URL}/{account_id}/insights"

    if INCREMENTAL_MODE:
        yesterday = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")
        time_range = {"since": yesterday, "until": yesterday}
    else:
        time_range = {"since": "2026-01-01", "until": "2026-04-27"}

    params = {
        "fields": ",".join(FIELDS),
        "level": "adset",
        "time_increment": 1,
        "limit": 500,
        "time_range": str(time_range).replace("'", '"'),
        "access_token": ACCESS_TOKEN
    }

    all_data = []

    while True:

        data = safe_request(url, params)

        if not data:
            break

        all_data.extend(data.get("data", []))

        if "next" in data.get("paging", {}):
            url = data["paging"]["next"]
            params = None
        else:
            break

        time.sleep(0.5)

    return all_data


# =========================
# MAIN
# =========================
def main():

    all_rows = []

    for account in AD_ACCOUNTS:
        print(f"Fetching data for: {account}")
        data = fetch_insights(account)
        all_rows.extend(data)

    df = pd.DataFrame(all_rows)

    if df.empty:
        print("❌ No data")
        return df

    numeric_cols = ["spend", "impressions", "clicks", "cpm", "ctr", "reach"]

    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df[df["spend"] > 0]

    df = df.rename(columns={
        "date_start": "Date",
        "campaign_name": "Campaign_Name",
        "adset_name": "Adset_Name",
        "spend": "Spend",
        "impressions": "Impressions",
        "clicks": "Clicks",
        "cpm": "CPM",
        "ctr": "CTR",
        "reach": "Reach"
    })

    df = df[
        [
            "Date",
            "Campaign_Name",
            "Adset_Name",
            "Spend",
            "Impressions",
            "Clicks",
            "CPM",
            "CTR",
            "Reach"
        ]
    ]

    return df


# =========================
# GOOGLE SHEETS (FIXED)
# =========================
def upload_to_gsheet(df):

    creds_json = os.getenv("GOOGLE_CREDS")

    if not creds_json:
        raise Exception("❌ GOOGLE_CREDS not found")

    creds_dict = json.loads(creds_json)

    creds = Credentials.from_service_account_info(
        creds_dict,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
    )

    client = gspread.authorize(creds)

    sheet = client.open_by_url(
        "https://docs.google.com/spreadsheets/d/1g8RmdoWssZmm00qkAokA-UjR2nhlcVakmwSpN-DF6xU/edit"
    ).worksheet("Sheet2")   # 🔥 CHANGE HERE IF NEEDED

    if INCREMENTAL_MODE:
        sheet.append_rows(df.values.tolist(), value_input_option="USER_ENTERED")
    else:
        sheet.clear()
        sheet.update([df.columns.values.tolist()] + df.values.tolist())


# =========================
# RUN
# =========================
if __name__ == "__main__":

    print("🚀 Running Spends Report...")

    df = main()

    if not df.empty:
        upload_to_gsheet(df)
        print("✅ Done Successfully!")
    else:
        print("❌ No data to upload")
