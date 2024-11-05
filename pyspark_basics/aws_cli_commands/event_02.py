# Write a Python script to create a rule in EventBridge that triggers every 5 minutes.

import boto3

eventbridge_client = boto3.client('events', region_name='ap-south-1')
try:
   rule_name = 'EveryFiveMinutesRule'
   response = eventbridge_client.put_rule(
      Name=rule_name,
      ScheduleExpression='rate(5 minutes)',
      State='ENABLED'
   )
   print(f"Rule '{rule_name}' created with ARN:", response['RuleArn'])
except Exception as e:
   print("Error creating EventBridge rule:", e)