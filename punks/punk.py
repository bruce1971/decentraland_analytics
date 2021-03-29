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
    for id in range(1, 10001):
        url = "https://api.opensea.io/api/v1/asset/0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb/" + str(id)
        punk = requests.request("GET", url).json()

        type_trait = [x for x in punk["traits"] if x["trait_type"] == "type"]
        type = type_trait[0]["value"] if len(type_trait) > 0 else None

        with conn.cursor() as cur:
            sql = f"""
            INSERT INTO punks(
                id
                ,type
                ,external_url
                ,opensea_url
            ) VALUES (
                "{id}",
                "{type}",
                "{punk["external_link"]}",
                "{punk["permalink"]}"
            )
            ON DUPLICATE KEY UPDATE
                id=id
            """
            cur.execute(sql)
        conn.commit()
        time.sleep(gap)
    conn.close()


lambda_handler({}, {})
