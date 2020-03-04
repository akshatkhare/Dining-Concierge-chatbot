import yelpapi
from yelpapi import YelpAPI
import json
import boto3
from decimal import Decimal
import requests
import datetime

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table("yelp_restaurants_1")

region = 'us-east-1'
service = 'es'

host = 'https://search-restaurants-kahefunwonk5zpz3i6q737vrnu.us-east-1.es.amazonaws.com'

index = 'restaurants'
type = 'Restaurant'

url = host + '/' + index + '/' + type + '/'

headers = { "Content-Type": "application/json" }

# write private api_key to access yelp here
api_key = 'fHNIyLqnzcT6zOx9S2ReGx82fXiuervdDr0tl6Su3Uzoonw8LQ8SOsVMn3tybHGX3_c47cr0WI5X4GBynHr91juJc19wYAZQUIK8-uv7Q96B0rqvHbgUI7AVSHBIXnYx'

yelp_api = YelpAPI(api_key)

data = ['id', 'name', 'review_count', 'rating', 'coordinates', 'address1', 'zip_code', 'display_phone']
es_data = ['id']

# cuisines = ["thai", "chinese", "mexican"]
# cuisines = [ "italian", "indian"]
# cuisines = ["american"]
cuisines = ["chinese"]


def fill_database(response, c):
    new_response = json.loads(json.dumps(response), parse_float=Decimal)
    for t in new_response["businesses"]:
        dict1 = { key:value for (key,value) in t.items() if key in data}
        dict2 = {key:value for (key,value) in t["location"].items() if key in data}
        dict1.update(dict2)
        dict1.update(cuisine=c)
        final_dict = {key: value for key, value in dict1.items() if value}
        timeStamp = str(datetime.datetime.now())
        final_dict.update(insertedAtTimestamp=timeStamp)
        
        my_es_id  = final_dict['id']
        es_dict = {key: final_dict[key] for key in final_dict.keys() & {'id', 'cuisine'}} 
        docs = json.loads(json.dumps(es_dict))
        
        r = requests.put(url+str(my_es_id), json=docs, headers=headers)
        print(r)
        table.put_item(Item=final_dict)
        

def lambda_handler(event=None, context=None):
    for c in cuisines:
        for x in range(0, 1000, 50):
            response = yelp_api.search_query(term=c, location='manhattan', limit=50, offset=x)
            fill_database(response, c)
