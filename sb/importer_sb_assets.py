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
    order_by = 'token_id'
    order_direction = 'desc'
    for offset in range(0, 10001, 50):
        print('offset:', offset)
        url = f"https://api.opensea.io/api/v1/assets?asset_contract_address=0x50f5474724e0ee42d9a4e711ccfb275809fd6d4a&collection=sandbox&order_by={order_by}&order_direction={order_direction}&offset={offset}&limit=50"
        print(url)
        lands = requests.request("GET", url).json()['assets']
        for land in lands:
            print(land['token_id'])
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
                    "{land['permalink']}",
                    "{land['external_link']}"
                )
                ON DUPLICATE KEY UPDATE
                    id=id;
                """
                cur.execute(sql)
            conn.commit()
        time.sleep(gap)
    conn.close()


lambda_handler({}, {})
