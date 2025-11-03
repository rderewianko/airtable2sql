import os
import json
import requests
import pandas as pd
from pyairtable import Api
from sqlalchemy import create_engine
import argparse


def sanitize_field(value):
    if isinstance(value, (list, dict)):
        return json.dumps(value)
    return value


def airtable_to_df(table_obj):
    records = table_obj.all()
    data = []
    for record in records:
        fields = record['fields']
        clean_fields = {k: sanitize_field(v) for k, v in fields.items()}
        clean_fields['_id'] = record['id']
        data.append(clean_fields)
    return pd.DataFrame(data)


def fetch_base_metadata(base_id, token):
    url = f"https://api.airtable.com/v0/meta/bases/{base_id}/tables"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch tables for base {base_id}: {response.status_code} {response.text}")
    return response.json()["tables"]


def get_all_bases(token):
    url = "https://api.airtable.com/v0/meta/bases"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch base list: {response.status_code} {response.text}")
    return response.json()["bases"]


def export_base_to_sqlite(base_id, base_name, output_dir, token):
    print(f"\n📦 Exporting base: {base_name} ({base_id})")

    tables_meta = fetch_base_metadata(base_id, token)
    api = Api(token)

    # Safe file name
    db_name = f"{base_name.replace(' ', '_')}.db"
    db_path = os.path.join(output_dir, db_name)
    engine = create_engine(f"sqlite:///{db_path}")

    for table in tables_meta:
        table_id = table['id']
        table_name = table['name'].replace(" ", "_")
        print(f"  🔄 Processing table '{table_name}'...")

        table_obj = api.table(base_id, table_id)
        df = airtable_to_df(table_obj)

        # Skip empty tables (no columns) to avoid SQL syntax errors
        if df.empty or len(df.columns) == 0:
            print(f"    ⚠️  Skipped (empty table)")
            continue

        df.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f"    💾 Saved")

    print(f"✅ Finished exporting '{base_name}' → {db_path}")


def main():
    parser = argparse.ArgumentParser(description="Download Airtable bases and store them in SQLite databases.")
    parser.add_argument(
        "-b", "--base",
        help="Comma-separated list of Airtable base IDs (e.g. appXXX,appYYY). "
             "If omitted, all accessible bases will be exported."
    )
    parser.add_argument(
        "-o", "--output",
        help="Output directory for database files (default: current directory)"
    )
    parser.add_argument(
        "-t", "--token",
        default=os.environ.get("AIRTABLE_API_KEY"),
        help="Airtable Personal Access Token (or set AIRTABLE_API_KEY environment variable)"
    )

    args = parser.parse_args()

    if not args.token:
        parser.error("No Airtable token provided. Use --token or set AIRTABLE_API_KEY environment variable.")

    output_dir = args.output or os.getcwd()
    os.makedirs(output_dir, exist_ok=True)

    # Get list of all accessible bases
    all_bases = get_all_bases(args.token)
    all_bases_dict = {b["id"]: b["name"] for b in all_bases}

    if args.base:
        base_ids = [b.strip() for b in args.base.split(",")]
        for base_id in base_ids:
            base_name = all_bases_dict.get(base_id)
            if not base_name:
                print(f"⚠️  Base ID '{base_id}' not found in your account — skipping.")
                continue
            export_base_to_sqlite(base_id, base_name, output_dir, args.token)
    else:
        for base_id, base_name in all_bases_dict.items():
            export_base_to_sqlite(base_id, base_name, output_dir, args.token)
