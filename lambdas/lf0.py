import json
import boto3

client = boto3.client('lex-runtime')

def lambda_handler(event, context):
    print("inside lambda 0")
    lastUserMessage = event['message'];
    botMessage = "There is something wrong, Please start process once again.";
    
    if lastUserMessage is None or len(lastUserMessage) < 1:
        return {
            'statusCode': 200,
            'body': json.dumps(botMessage)
        }
    
    # Update the user id, so it is different for different user
    response = client.post_text(botName='DiningConcierge',
        botAlias='chatbotConcierge',
        userId='ak7674',
        inputText=lastUserMessage)
    
    if response['message'] is not None or len(response['message']) > 0:
        lastUserMessage = response['message']
    
    print("Last user message", lastUserMessage)
    
    return {
        'statusCode': 200,
        'body': json.dumps(lastUserMessage)
    }
