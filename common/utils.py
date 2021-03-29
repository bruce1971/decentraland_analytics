import pymysql
from urllib.request import urlopen, Request


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
