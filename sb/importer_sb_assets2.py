import requests
import time
import sys
import pymysql
import datetime
sys.path.insert(0, './common')
from utils import connect_to_db
gap = 1


def lambda_handler(event, context):
    conn = connect_to_db()
    for token_id in range(12350, 156000):
        time.sleep(0.2)
        print('============================')
        print('token_id:', token_id)
        url = f"https://api.opensea.io/api/v1/asset/0x50f5474724e0ee42d9a4e711ccfb275809fd6d4a/{token_id}"
        land = requests.request("GET", url).json()

        if 'success' in land:
            print('meh...')
            continue
        elif 'id' not in land:
            print(land)
            time.sleep(1)
            continue
        elif len([t['value'] for t in land["traits"] if t["trait_type"] == 'x']) == 0:
            print(land)
            time.sleep(1)
            continue

        print('LAAAAAAAAAAAAAAAAAAAND!')
        with conn.cursor() as cur:
            sql = f"""
            INSERT INTO sb_assets (
                id,
                token_id,
                x,
                y,
                external_url,
                opensea_url
            ) VALUES (
                {land['id']},
                "{land['token_id']}",
                {[t['value'] for t in land["traits"] if t["trait_type"] == 'x'][0]},
                {[t['value'] for t in land["traits"] if t["trait_type"] == 'y'][0]},
                "{land['external_link']}",
                "{land['permalink']}"
            )
            ON DUPLICATE KEY UPDATE
                id=id;
            """
            cur.execute(sql)
        conn.commit()
    conn.close()


lambda_handler({}, {})
