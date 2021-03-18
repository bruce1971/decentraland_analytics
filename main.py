import requests


url = "https://api.opensea.io/api/v1/events"
querystring = {
    "only_opensea": "false",
    "offset":"0",
    "limit":"3",
    "event_type": "successful",
    "collection_slug": "decentraland"
}
events = requests.request("GET", url, params=querystring).json()


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
    row = {
        "tx_id": event["transaction"]["timestamp"],
        "creation_timestamp": event["created_date"],
        "sale_timestamp": event["transaction"]["timestamp"],
        "price_usd": round(price_mana * float(event["payment_token"]["usd_price"])),
        "price_eth": round(price_mana * float(event["payment_token"]["eth_price"]), 2),
        "price_mana": round(price_mana),
        "location": event["asset"]["image_original_url"],
        "seller_address": event["seller"]["address"],
        "buyer_address": event["winner_account"]["address"],
        "land_type": land_type,
        "size": [x for x in asset["traits"] if x["trait_type"] == "Size"][0]["value"] if land_type == "Estate" else 1,
        "distance_to_road": -1,
        "distance_to_district": -1,
        "distance_to_plaza": -1
    }
    print(row)
