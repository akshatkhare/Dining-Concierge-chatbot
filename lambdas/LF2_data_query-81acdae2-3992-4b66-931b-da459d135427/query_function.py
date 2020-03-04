#LF2_Call code
import json
import boto3
import boto3
import requests
from boto3.dynamodb.conditions import Key



def get_result(input_data):
    records = input_data['Records']
    print("before printing input_data")
    print(input_data['Records'])
    print("after printing input_data")
    json_record = records[0]
    lambda_client = boto3.client('lambda')
    dynamodb = boto3.resource('dynamodb')
    tableName = "yelp_restaurants_1"
    table = dynamodb.Table(tableName)

    region = 'us-east-1'
    service = 'es'
    host = 'https://search-restaurants-kahefunwonk5zpz3i6q737vrnu.us-east-1.es.amazonaws.com'
    
    index = 'restaurants'
    type = 'Restaurant'
    
    url = host + '/' + index + '/_search?'
    
    headers = headers = {
        'Content-Type': "application/json",
        'Accept': "/",
        'Cache-Control': "no-cache",
        'Host': "search-restaurants-kahefunwonk5zpz3i6q737vrnu.us-east-1.es.amazonaws.com",
        'Accept-Encoding': "gzip, deflate",
        'Content-Length': "335",
        'Connection': "keep-alive",
        'cache-control': "no-cache"
        }
    
    cuisine = str(json_record['messageAttributes']['Cuisine']['stringValue'])
    
    search_params = """{"query":{"bool":{"must":[{"match":{"cuisine":\"""" + cuisine + """\"}}],"must_not":[],"should":[]}},"from":0,"size":3,"sort":[],"aggs":{}}"""
    response = requests.get(url=url, data=search_params, headers=headers)
    print("printing resopnse")
    print(response.text, '\n')
    print("after resp")
    dict1 = json.loads(response.text)
    print("before dict")
    print ("dictionary :",dict1)
    print("after dict")
    records = []
    for i in dict1.get("hits").get("hits"):
        x = table.query(KeyConditionExpression=Key('id').eq(i.get("_id")))
        res_name = x['Items'][0]['name']
        res_address = x['Items'][0]['address1']
        # print(x, '\n')
        records.append({'name' : res_name, 'address' : res_address})
    # print (records)
    return records
    

def message_template(common_data, msg_content):
    c_data_cuisine = common_data['Records'][0]['messageAttributes']['Cuisine']['stringValue']
    c_data_people = common_data['Records'][0]['messageAttributes']['Number']['stringValue']
    c_data_date = common_data['Records'][0]['messageAttributes']['Date']['stringValue']
    c_data_time = common_data['Records'][0]['messageAttributes']['Time']['stringValue']
    print (message_template)
    content ='Hello! Here are my ' + c_data_cuisine + ' restaurant suggestions for ' + c_data_people + ' people, ' + c_data_date + ' at '+ c_data_time +'.\n'
    for i in range(len(msg_content)):
        content += (str(i+1) +'. '+ msg_content[i]['name'] + ', located at ' + msg_content[i]['address']) 
        content += '\n'
    print (content)
    return content
    
def send_to_sns(common_data, raw_data):
    ses = boto3.client('ses')
    sns = boto3.client('sns')
    # print (event)
    topic_name = 'Send-email'
    # topic = sns.create_topic(Name = topic_name)
    msg = message_template(common_data,raw_data)
    print("phone-error")
    c_data_phone = "+1"+common_data['Records'][0]['messageAttributes']['PhoneNumber']['stringValue']
    print("phone-number")
    print(c_data_phone)
    #c_email = common_data['Records'][0]['messageAttributes']['Email']['stringValue']
    tpcArn = 'arn:aws:sns:us-east-1:106689315603:' + topic_name
    
    
    #SMS Template
    # contact = '+1'+c_data_phone
    # subs = sns.subscribe(
    #     TopicArn=tpcArn,
    #     Protocol='email',
    #     Endpoint= 'mbjori@gmail.com'  # <-- number who'll receive an SMS message.
    # )
    # response = sns.publish(
    # TopicArn = 'arn:aws:sns:us-east-1:777754410071:userNotification',    
    # Message=msg)
    
    
    
    # Email Template
    # contact = c_data_phone
    '''
    subs = sns.subscribe(
        TopicArn=tpcArn,
        Protocol='email',
        Endpoint= c_email  # <-- number who'll receive an SMS message.
    )

    response = sns.publish(
    TopicArn = 'arn:aws:sns:us-east-1:106689315603:Send-email',    
    Message=msg)
    ''' 
    subs = sns.subscribe(
        TopicArn=tpcArn,
        Protocol='SMS',
        Endpoint= c_data_phone  # <-- number who'll receive an SMS message.
    )

    response = sns.publish(
    TopicArn = 'arn:aws:sns:us-east-1:106689315603:Send-email',    
    Message=msg)
    
        
    # # Print out the response
    # print("SNS Response:", response)
    
    # ses = boto3.client('ses')

    # response = ses.verify_email_identity(
    #   EmailAddress = c_email
    # )
    # print (response)
    
    # response = ses.send_email(
    #   Source          = 'mbjori@gmail.com',
    #   Destination     = {
        
    #     'ToAddresses' : [
    #       c_email,
    #     ]
    #   },
    
    #   Message = {
    #     'Subject'    : {
    #       'Data'     : 'Restaurant Notification',
    #       'Charset'  : 'UTF-8'
    #     },
    #     'Body'       : {
    #       'Text'     : {
    #         'Data'   : msg,
    #         'Charset': 'UTF-8'
    #       }
    #     }
    #   }
    # )
    
    # print(response)
     
    
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }


def query_elastic(event, context):
    print (event)
    #print(event['Records'])
    #print(event['Records'][0])
    #print(event['Records'][0]['messageAttributes'])
    #print(event['Records'][0]['messageAttributes']['Cuisine'])
    #print(event['Records'][0]['messageAttributes']['Cuisine']['stringValue'])
    result = get_result(event)
    sent_message = send_to_sns(event, result)
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
