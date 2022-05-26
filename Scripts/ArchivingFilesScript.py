import boto3
from datetime import datetime, timedelta

SOURCE_BUCKET = 'bucket-a'
DESTINATION_BUCKET = 'bucket-b'

s3_client = boto3.client('s3')

# Create a reusable Paginator
paginator = s3_client.get_paginator('list_objects_v2')

# Create a PageIterator from the Paginator
page_iterator = paginator.paginate(Bucket=SOURCE_BUCKET)

# Loop through each object, looking for ones older than a given time period
for page in page_iterator:
    for object in page['Contents']:
        if object['LastModified'] < datetime.now().astimezone() - timedelta(days=2):   # <-- Change time period here
            print(f"Moving {object['Key']}")

            # Copy object
            s3_client.copy_object(
                Bucket=DESTINATION_BUCKET,
                Key=object['Key'],
                CopySource={'Bucket':SOURCE_BUCKET, 'Key':object['Key']}
            )

            # Delete original object
            s3_client.delete_object(Bucket=SOURCE_BUCKET, Key=object['Key'])