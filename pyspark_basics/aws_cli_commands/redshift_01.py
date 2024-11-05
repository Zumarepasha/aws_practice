# Write a Python program to list all Amazon Redshift clusters in your AWS account using Boto3.

import boto3
redshift_client = boto3.client('redshift', region_name='ap-south-1')
try:
    response = redshift_client.describe_clusters()    
    clusters = response.get('Clusters', [])    
    if clusters:
        print("Amazon Redshift Clusters:")
        for cluster in clusters:
            cluster_identifier = cluster['ClusterIdentifier']
            cluster_status = cluster['ClusterStatus']
            print(f"Cluster Identifier: {cluster_identifier}, Status: {cluster_status}")
    else:
        print("No Redshift clusters found in your account.")        
except Exception as e:
    print(f"An error occurred: {e}")