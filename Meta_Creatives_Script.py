# ============================================================
# META CREATIVE REPORT SCRIPT (FINAL - GITHUB READY)
# ============================================================

import requests
import pandas as pd
import time
import gspread
import os
import json
from google.oauth2.service_account import Credentials
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta


# =========================
# CONFIG
# =========================

BASE_URL = "https://graph.facebook.com/v18.0"

INCREMENTAL_MODE = True

# ✅ KEEPING YOUR ORIGINAL TOKENS
ACCOUNT_TOKEN_MAP = {

    "EAAHKYZBFiGZCoBQz5f6iCWOZBBmKt5Mm0neEsrqX36cZBhz039hz8ur8d3JZCldzVKLrZByQj233n7NMGDoDO3JZACefKMVD3UhGKFBtmi5wwyZCphr1I6xYBYXY0mnnQIMporwu7BYZA8by4LQHyZCCVGdpf0pZBquDBSa5R3ls388LDaZCa5brMmYl21tj6PxuudnP": [
        "act_440162118348238",
        "act_1719931635154497",
        "act_718781920169137",
        "act_701631941903775",
        "act_1501970208143735",
        "act_1315886119065844",
        "act_336496342055409",
        "act_636982851281381",
        "act_3013682562268050",
        "act_999222314375640",
        "act_584043403558805",
        "act_441229168101305",
        "act_241562334",
        "act_750729814964276"
    ],

    "EAAX6VtO5tO8BRGQjZCOmZA5UDfsHa2SzZAvbaLX9X6n36stZAyZA4PBcgZA0ZARdSgrcm6swHYyYq0SXwrZAZBStK3EJIUQLmddCNFvCB2ZA3nPy9Ao2n0hu4MpitzMoanDhXlGGQt1J738vFZBwls5fZCk7o2Q9tZBaPt5HO2dOQU7ZChBPiCNaK8eZBIIeZB8kFpQ4UpXE": [
        "act_61747633",
        "act_1638356833183465",
        "act_375509061368472",
        "act_575017180766466"
    ]
}

FIELDS = [
    "date_start", "campaign_name", "ad_name", "adset_name",
    "ad_id", "spend", "impressions", "clicks",
    "cpm", "ctr", "reach"
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
                time.sleep(3)
                continue

            return res

        except Exception as e:
            print("Request failed:", e)
            time.sleep(3)

    return None


# =========================
# FETCH INSIGHTS
# =========================
def fetch_insights(account_id, token):

    url = f"{BASE_URL}/{account_id}/insights"

    if INCREMENTAL_MODE:
        yesterday = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")
        time_range = {"since": yesterday, "until": yesterday}
    else:
        time_range = {"since": "2026-04-01", "until": "2026-04-27"}

    params = {
        "fields": ",".join(FIELDS),
        "level": "ad",
        "time_increment": 1,
        "limit": 500,
        "time_range": str(time_range).replace("'", '"'),
        "access_token": token
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

        time.sleep(0.3)

    return all_data


# =========================
# FETCH CREATIVES
# =========================
def fetch_creatives(ad_ids, token):

    def worker(ad_id):
        url = f"{BASE_URL}/{ad_id}"
        params = {
            "fields": "adcreatives{image_url,thumbnail_url}",
            "access_token": token
        }

        res = safe_request(url, params)

        try:
            creative = res["adcreatives"]["data"][0]
            return ad_id, creative.get("image_url") or creative.get("thumbnail_url")
        except:
            return ad_id, None

    creative_map = {}

    with ThreadPoolExecutor(max_workers=10) as executor:
        for ad_id, url in executor.map(worker, ad_ids):
            creative_map[ad_id] = url

    return creative_map


# =========================
# FETCH STATUS
# =========================
def fetch_status(ad_ids, token):

    def worker(ad_id):
        url = f"{BASE_URL}/{ad_id}"
        params = {
            "fields": "effective_status",
            "access_token": token
        }

        res = safe_request(url, params)
        return ad_id, res.get("effective_status") if res else None

    status_map = {}

    with ThreadPoolExecutor(max_workers=10) as executor:
        for ad_id, status in executor.map(worker, ad_ids):
            status_map[ad_id] = status

    return status_map


# =========================
# MAIN
# =========================
def main():

    all_rows = []

    for token, accounts in ACCOUNT_TOKEN_MAP.items():

        print(f"\n🔑 Processing Token: {token[:10]}...")

        for account in accounts:

            print(f"Fetching data for: {account}")

            data = fetch_insights(account, token)

            for row in data:
                row["token_used"] = token

            all_rows.extend(data)

    df = pd.DataFrame(all_rows)

    if df.empty:
        print("❌ No data")
        return df

    numeric_cols = ["spend", "impressions", "clicks", "cpm", "ctr", "reach"]

    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df[df["impressions"] > 0]

    print("Fetching creatives & status...")

    creative_map = {}
    status_map = {}

    for token in df["token_used"].unique():

        token_df = df[df["token_used"] == token]

        ad_ids = token_df["ad_id"].dropna().unique().tolist()

        creative_map.update(fetch_creatives(ad_ids, token))
        status_map.update(fetch_status(ad_ids, token))

    df["Creative_URL"] = df["ad_id"].map(creative_map)
    df["Status"] = df["ad_id"].map(status_map)

    df = df.rename(columns={
        "date_start": "Date",
        "campaign_name": "Campaign_Name",
        "adset_name": "Adset_Name",
        "ad_name": "Ad_Name",
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
            "Ad_Name",
            "Status",
            "Creative_URL",
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

    # ✅ FIXED: Use GitHub Secret instead of local file
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

    # ✅ FIXED: Use Sheet2 safely
    sheet = client.open("Meta Creatives Data from Python Script").worksheet("Sheet1")

    if INCREMENTAL_MODE:

        print("📌 Appending data")

        sheet.append_rows(
            df.values.tolist(),
            value_input_option="USER_ENTERED"
        )

    else:

        print("🔄 Full refresh")

        sheet.clear()

        sheet.update(
            [df.columns.values.tolist()] + df.values.tolist(),
            value_input_option="USER_ENTERED"
        )


# =========================
# RUN
# =========================
if __name__ == "__main__":

    print("🚀 Starting Final Meta Report...")

    df = main()

    if not df.empty:
        upload_to_gsheet(df)
        print("✅ Done Successfully!")
    else:
        print("❌ No data to upload")
