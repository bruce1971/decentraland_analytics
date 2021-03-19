import requests
import time


url = "https://api.opensea.io/api/v1/events"
querystring = {
    "only_opensea": "false",
    "offset":"0",
    "limit":"15",
    "collection_slug": "decentraland",
    # "occurred_before":"20000",
    # "occurred_after": time.time() - 1000000
    "event_type": "successful"
}
events = requests.request("GET", url, params=querystring).json()


rows = []
for event in events["asset_events"]:
    asset_url = "https://api.opensea.io/api/v1/assets"
    querystring = {
        "token_ids": event["asset"]["token_id"],
        "asset_contract_address": event["asset"]["asset_contract"]["address"]
    }
    assets = requests.request("GET", asset_url, params=querystring).json()
    asset = assets["assets"][0]

    price_mana = int(event["total_price"])/1000000000000000000
    land_type = [x for x in asset["traits"] if x["trait_type"] == "Type"][0]["value"]
    size = [x for x in asset["traits"] if x["trait_type"] == "Size"][0]["value"] if land_type == "Estate" else 1
    price_usd = price_mana * float(event["payment_token"]["usd_price"])

    row = {
        "sale_timestamp": event["transaction"]["timestamp"],
        "price_usd": round(price_usd),
        "size": size,
        "price_usd_parcel": round(price_usd/size),
        "price_eth": round(price_mana * float(event["payment_token"]["eth_price"]), 2),
        "price_mana": round(price_mana),
        "land_type": land_type,
        "distance_to_road": -1,
        "distance_to_district": -1,
        "distance_to_plaza": -1,
        "coordinates": -1,
        "dcl_url": event["asset"]["external_link"],
        "opensea_url": event["asset"]["permalink"],
        "seller_address": event["seller"]["address"],
        "buyer_address": event["winner_account"]["address"],
        "tx_id": event["transaction"]["transaction_hash"]
    }
    rows.append(row)


print(rows)
