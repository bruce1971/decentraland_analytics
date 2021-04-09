import requests
import time
import sys
import pymysql
import datetime
sys.path.insert(0, './common')
from utils import connect_to_db, price_feed
gap = 1.5


def import_events(conn, querystring, eth_usd_dict):
    url = "https://api.opensea.io/api/v1/events"
    events = requests.request("GET", url, params=querystring).json()
    with conn.cursor() as cur:
        now_timestamp = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        for event in events["asset_events"]:
            if event["asset"] is None:
                continue
            if event['asset']['asset_contract']['asset_contract_type'] != 'non-fungible':
                continue
            if event['payment_token']['symbol'] not in ['ETH', 'WETH']:
                continue

            event_timestamp = event["transaction"]["timestamp"]
            amount_eth = int(event['total_price'])/1e18
            amount_usd = amount_eth * eth_usd_dict[event_timestamp[:10]]
            seller_address = event['seller']['address']
            buyer_address = event['winner_account']['address']

            sql = f"""
            INSERT INTO sb_events(
                id,
                asset_id,
                asset_token_id,
                event_type,
                event_timestamp,
                amount_eth,
                amount_usd,
                seller_address,
                buyer_address,
                opensea_url,
                updated_timestamp
            ) VALUES (
                "{event['id']}",
                {int(event['asset']['id'])},
                "{event['asset']['token_id']}",
                "{event["event_type"]}",
                "{event_timestamp}",
                {amount_eth if amount_eth is not None else 'NULL'},
                {int(amount_usd) if amount_usd is not None else 'NULL'},
                {f"'{seller_address}'" if seller_address is not None else 'NULL'},
                {f"'{buyer_address}'" if buyer_address is not None else 'NULL'},
                "{event['asset']['permalink']}",
                "{now_timestamp}"
            )
            ON DUPLICATE KEY UPDATE
                event_timestamp = "{event["transaction"]["timestamp"]}",
                amount_eth = {amount_eth if amount_eth is not None else 'NULL'},
                amount_usd = {int(amount_usd) if amount_usd is not None else 'NULL'},
                updated_timestamp = "{now_timestamp}"
            """
            cur.execute(sql)
            print('Inserting event', event["id"])
    conn.commit()


def lambda_handler(event, context):
    conn = connect_to_db()
    jump = 6*3600 #1hours
    start_time = event['start_time'] if 'start_time' in event else int(time.time()) #now
    slots = event['slots'] if 'slots' in event else 30 #now
    timeslots = []
    for i in range(0, slots):
        timeslots.append([start_time - jump*(i+1), start_time - jump*i])

    eth_usd_dict = price_feed("1027", "USD") # eth -> usd

    for timeslot in timeslots:
        print('Datetime:', datetime.datetime.utcfromtimestamp(timeslot[1]).strftime('%Y-%m-%d %H:%M:%S'))
        print('Timeslot: ', timeslot)
        querystring = {
            "only_opensea": "false",
            "offset": "0",
            "limit": "10000",
            "event_type": "successful",
            "collection_slug": "sandbox",
            "asset_contract_address": "0x50f5474724e0ee42d9a4e711ccfb275809fd6d4a",
            "occurred_before": timeslot[1],
            "occurred_after": timeslot[0]
        }
        import_events(conn, querystring, eth_usd_dict)
        time.sleep(gap)

    conn.close()


event = { 'start_time': 1615146899, 'slots': 9999 }
# event = { 'slots': 300 }
lambda_handler(event, {})
