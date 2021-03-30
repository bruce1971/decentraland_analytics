import pymysql
import time
import requests
import datetime


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


def price_feed(convert_from_id, convert_to_type):
    print('Fetching historical price feeds for', convert_from_id, '...')
    url = 'https://web-api.coinmarketcap.com/v1/cryptocurrency/ohlcv/historical'
    querystring = {
        "id": convert_from_id,
        "convert": convert_to_type,
        "time_start": "1311100800",
        "time_end": int(time.time())
    }
    data = requests.request("GET", url, params=querystring).json()
    formatted_dict = {}
    for quote in data["data"]["quotes"]:
        formatted_dict[quote["time_close"][:10]] = quote["quote"][convert_to_type]["close"]
    last_quote = data["data"]["quotes"][-1]
    current_date = str(datetime.datetime.strptime(last_quote["time_close"][:10], "%Y-%m-%d") + datetime.timedelta(days=1))[:10]
    formatted_dict[current_date] = last_quote["quote"][convert_to_type]["close"]
    return formatted_dict
