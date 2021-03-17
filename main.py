import requests

url = "https://api.opensea.io/api/v1/events"

querystring = {"only_opensea":"false","offset":"0","limit":"320"}

response = requests.request("GET", url, params=querystring)

print(response.text)
