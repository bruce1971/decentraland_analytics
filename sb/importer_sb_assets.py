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
    jump = 20
    start = event['start'] if 'start' in event else 0
    slots = event['slots'] if 'slots' in event else 5
    rangeslots = []
    for i in range(0, slots):
        rangeslots.append([start + jump*i, start + jump*(i+1) - 1])
    for rangeslot in rangeslots:
        print('rangeslot:', rangeslot)
        url = f"https://api.opensea.io/api/v1/assets?asset_contract_address=0x50f5474724e0ee42d9a4e711ccfb275809fd6d4a&collection=sandbox&order_direction=desc&offset={rangeslot[0]}&limit=50"
        print(url)
        # print(requests.request("GET", url).json())
        lands = requests.request("GET", url).json()['assets']
        for land in lands:
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


lambda_handler({'start': 0, 'slots': 10000}, {})
