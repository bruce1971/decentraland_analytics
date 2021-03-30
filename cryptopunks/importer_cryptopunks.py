import requests
import time
import sys
import pymysql
import datetime
sys.path.insert(0, './common')
from utils import connect_to_db, price_feed
gap = 3


def import_events(conn, querystring, eth_usd_dict):

    url = "https://api.opensea.io/api/v1/events"
    events = requests.request("GET", url, params=querystring).json()

    with conn.cursor() as cur:
        now_timestamp = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        for event in events["asset_events"]:
            if event["event_type"] == 'created':
                amount_eth = float(event["starting_price"])/1e18
                amount_usd = amount_eth * eth_usd_dict[event["created_date"][:10]]
                seller_address = event["asset"]["owner"]["address"]
                buyer_address = None
            # elif event["event_type"] == 'successful':
            #     amount_eth = float(event["starting_price"])/1e18
            #     amount_usd = amount_eth * eth_usd_dict[event["created_date"][:10]]
            #     seller_address = event["asset"]["owner"]["address"]
            #     buyer_address = None
            else:
                amount_eth = 999
                amount_usd = 888
                seller_address = None
                buyer_address = None

            sql = f"""
            INSERT INTO cryptopunks_events(
                id,
                cryptopunk_id,
                event_type,
                event_timestamp,
                amount_eth,
                amount_usd,
                seller_address,
                buyer_address,
                updated_timestamp
            ) VALUES (
                "{event["id"]}",
                {int(event["asset"]["token_id"])},
                "{event["event_type"]}",
                "{event["created_date"]}",
                {amount_eth},
                {int(amount_usd)},
                {f"'{seller_address}'" if seller_address is not None else 'NULL'},
                {f"'{buyer_address}'" if buyer_address is not None else 'NULL'},
                "{now_timestamp}"
            )
            ON DUPLICATE KEY UPDATE
                id=id
            """
            print(sql)
            cur.execute(sql)
            print('Inserting event', event["id"])
    conn.commit()


def lambda_handler(event, context):
    conn = connect_to_db()
    jump = 21600 #6hours
    current = event['start_time'] if 'start_time' in event else int(time.time()) #now
    timeslots = []
    for i in range(0, 4*365):
        timeslots.append([current - jump*(i+1), current - jump*i])

    eth_usd_dict = price_feed("1027", "USD") # eth -> usd

    for timeslot in timeslots:
        print('Datetime:', datetime.datetime.utcfromtimestamp(timeslot[1]).strftime('%Y-%m-%d %H:%M:%S'))
        print('Timeslot: ', timeslot)
        querystring = {
            "only_opensea": "false",
            "offset": "0",
            "collection_slug": "cryptopunks",
            "event_type": "successful",
            "occurred_before": timeslot[1],
            "occurred_after": timeslot[0]
        }
        import_events(conn, querystring, eth_usd_dict)
        time.sleep(gap)

    conn.close()


# event = { 'start_time': 1606592354 }
event = {}
lambda_handler(event, {})
