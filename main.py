import requests
import time
import sys
import pymysql
import datetime
gap = 1


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

def import_sales(conn, querystring):

    print('Start api requests...')
    url = "https://api.opensea.io/api/v1/events"
    events = requests.request("GET", url, params=querystring).json()

    rows = []
    for event in events["asset_events"]:
        if event["asset"] is None:
            continue
        asset_url = "https://api.opensea.io/api/v1/assets"
        querystring2 = {
            "token_ids": event["asset"]["token_id"],
            "asset_contract_address": event["asset"]["asset_contract"]["address"]
        }
        assets = requests.request("GET", asset_url, params=querystring2).json()
        asset = assets["assets"][0]

        price_mana = int(event["total_price"])/1000000000000000000
        land_type = [x for x in asset["traits"] if x["trait_type"] == "Type"][0]["value"]
        size = [x for x in asset["traits"] if x["trait_type"] == "Size"][0]["value"] if land_type == "Estate" else 1
        if size == 0:
            continue
        price_usd = price_mana * float(event["payment_token"]["usd_price"])
        price_eth = price_mana * float(event["payment_token"]["eth_price"])
        distance_to_road_trait = [x for x in asset["traits"] if x["trait_type"] == "Distance to Road"]
        distance_to_road = distance_to_road_trait[0]["value"] if len(distance_to_road_trait) > 0 else None
        distance_to_district_trait = [x for x in asset["traits"] if x["trait_type"] == "Distance to District"]
        distance_to_district = distance_to_district_trait[0]["value"] if len(distance_to_district_trait) > 0 else None
        distance_to_plaza_trait = [x for x in asset["traits"] if x["trait_type"] == "Distance to Plaza"]
        distance_to_plaza = distance_to_plaza_trait[0]["value"] if len(distance_to_plaza_trait) > 0 else None

        row = {
            "sale_timestamp": event["transaction"]["timestamp"],
            "size": size,
            "price_usd": round(price_usd),
            "price_usd_parcel": round(price_usd/size),
            "price_eth": round(price_eth, 3),
            "price_eth_parcel": round(price_eth/size, 3),
            "price_mana": round(price_mana),
            "price_mana_parcel": round(price_mana/size),
            "land_type": land_type,
            "distance_to_road": distance_to_road,
            "distance_to_district": distance_to_district,
            "distance_to_plaza": distance_to_plaza,
            "dcl_url": event["asset"]["external_link"],
            "opensea_url": event["asset"]["permalink"],
            "seller_address": event["seller"]["address"],
            "buyer_address": event["winner_account"]["address"],
            "tx_id": event["transaction"]["transaction_hash"]
        }
        rows.append(row)
        print("Imported:", event["transaction"]["transaction_hash"])
        time.sleep(gap)


    print("Start inserting sales...")
    with conn.cursor() as cur:
        now_timestamp = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        for row in rows:
            sql = f"""
            INSERT INTO sales(
                sale_timestamp,
                size,
                price_usd,
                price_usd_parcel,
                price_eth,
                price_eth_parcel,
                price_mana,
                price_mana_parcel,
                land_type,
                distance_to_road,
                distance_to_district,
                distance_to_plaza,
                dcl_url,
                opensea_url,
                seller_address,
                buyer_address,
                tx_id,
                updated_timestamp
            ) VALUES (
                "{row["sale_timestamp"]}",
                "{row["size"]}",
                "{row["price_usd"]}",
                "{row["price_usd_parcel"]}",
                "{row["price_eth"]}",
                "{row["price_eth_parcel"]}",
                "{row["price_mana"]}",
                "{row["price_mana_parcel"]}",
                "{row["land_type"]}",
                {row['distance_to_road'] if row['distance_to_road'] is not None else 'NULL'},
                {row['distance_to_district'] if row['distance_to_district'] is not None else 'NULL'},
                {row['distance_to_plaza'] if row['distance_to_plaza'] is not None else 'NULL'},
                "{row["dcl_url"]}",
                "{row["opensea_url"]}",
                "{row["seller_address"]}",
                "{row["buyer_address"]}",
                "{row["tx_id"]}",
                "{now_timestamp}"
            )
            ON DUPLICATE KEY UPDATE
                sale_timestamp = "{row["sale_timestamp"]}",
                size = "{row["size"]}",
                price_usd = "{row["price_usd"]}",
                price_usd_parcel = "{row["price_usd_parcel"]}",
                price_eth = "{row["price_eth"]}",
                price_eth_parcel = "{row["price_eth_parcel"]}",
                price_mana = "{row["price_mana"]}",
                price_mana_parcel = "{row["price_mana_parcel"]}",
                updated_timestamp = "{now_timestamp}"
            """
            cur.execute(sql)

    # persist data
    conn.commit()
    print("Successfully inserted sales => ", len(rows))


def run():
    conn = connect_to_db()
    jump = 21600 #6hours
    current = 1616148193
    timeslots = []
    for i in range(0, 365*4):
        timeslots.append([current - jump*(i+1), current - jump*i])

    for timeslot in timeslots:
        print('Timeslot: ', timeslot)
        querystring = {
            "only_opensea": "false",
            "offset":"0",
            "collection_slug": "decentraland",
            "occurred_before": timeslot[1],
            "occurred_after": timeslot[0],
            "event_type": "successful"
        }
        import_sales(conn, querystring)
        time.sleep(gap)

    conn.close()


run()
