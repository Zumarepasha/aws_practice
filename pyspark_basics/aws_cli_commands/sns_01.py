#Write a Python program to list all SNS topics in your AWS account and print their ARNs.


import boto3

def list_sns_topics():
    # Create an SNS client
    sns_client = boto3.client('sns')

    # Call list_topics to retrieve the list of SNS topics
    response = sns_client.list_topics()

    # Check if there are any topics
    if 'Topics' in response:
        topics = response['Topics']
        print("SNS Topics and their ARNs:")
        for topic in topics:
            print(topic['TopicArn'])
    else:
        print("No SNS topics found.")

list_sns_topics()