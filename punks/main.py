import requests
import time
import sys
import pymysql
import datetime
gap = 2


def connect_to_db():
    host = "ftt-db-dev.cvivbsbheldp.eu-west-1.rds.amazonaws.com"
    user = "root"
    password = "Q5E36RzvmqT7"
    db_name = "ftt"
    try:
        conn = pymysql.connect(host=host, user=user, passwd=password, db=db_name, connect_timeout=10)
        print("SUCCESS: Connection to RDS MySQL instance succeeded")
        return conn
    except:
        print("ERROR: Unexpected error: Could not connect to MySQL instance.")
        sys.exit()
        return


def price_feed(type):
    print('Fetching historical price feeds...')
    url = 'https://web-api.coinmarketcap.com/v1/cryptocurrency/ohlcv/historical'
    querystring = {
        "id": "1027",
        "convert": type,
        "time_start": "1311100800",
        "time_end": int(time.time())
    }
    data = requests.request("GET", url, params=querystring).json()
    formatted_dict = {}
    for quote in data["data"]["quotes"]:
        formatted_dict[quote["time_close"][:10]] = quote["quote"][type]["close"]
    last_quote = data["data"]["quotes"][-1]
    current_date = str(datetime.datetime.strptime(last_quote["time_close"][:10], "%Y-%m-%d") + datetime.timedelta(days=1))[:10]
    formatted_dict[current_date] = last_quote["quote"][type]["close"]
    return formatted_dict


def import_events(conn, querystring, eth_dict):

    print('Start api requests...')
    url = "https://api.opensea.io/api/v1/events"
    events = requests.request("GET", url, params=querystring).json()

    print("Start inserting events...")
    with conn.cursor() as cur:
        now_timestamp = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        for event in events["asset_events"]:
            sql = f"""
            INSERT INTO punks_events(
              event_timestamp
              ,price_eth
              ,price_usd
              ,external_url
              ,opensea_url
              ,event_id
            ) VALUES (
            "{event["created_date"]}",
            {int(event["total_price"])/1e18},
            {99999},
            "{event["asset"]["external_link"]}",
            "{event["asset"]["permalink"]}",
            "{event["id"]}"
            )
            ON DUPLICATE KEY UPDATE
                event_id=event_id
            """
            cur.execute(sql)

    # persist data
    conn.commit()
    # print("Successfully inserted sales => ", len(rows))


def lambda_handler(event, context):
    conn = connect_to_db()
    jump = 21600 #6hours
    current = event['start_time'] if 'start_time' in event else int(time.time()) #now
    timeslots = []
    for i in range(0, 4*365):
        timeslots.append([current - jump*(i+1), current - jump*i])

    eth_dict = price_feed("ETH")

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
        import_events(conn, querystring, eth_dict)
        time.sleep(gap)

    conn.close()


# event = { 'start_time': 1606592354 }
event = {}
lambda_handler(event, {})
