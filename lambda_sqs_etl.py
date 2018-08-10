import collections
import json
import os
import uuid

import boto3


def handler_list_pages(event, context):
    """
    This handler lists all pages in a S3 bucket and sends each individual page
    as a SQS message into the Pages SQS queue. A page is a group of up to 1000
    S3 object keys.

    Listing all pages in a S3 bucket can take more than 5 minutes,
    which exceeds AWS Lambda's execution time limit.
    For this reason, this handler is re-invoked by a state machine
    until all pages of the S3 bucket have been listed.
    """

    event['all_pages_listed'] = 'FALSE'

    queue = boto3.resource('sqs').Queue(os.environ['PAGES_SQS_QUEUE_URL'])
    bucket = boto3.resource('s3').Bucket(os.environ['SOURCE_BUCKET_NAME'])

    filter_kwargs = {
        'MaxKeys': 1000,
    }

    # The bookmark is set before re-invocation
    bookmark = event.get('bookmark', None)
    if bookmark:
        filter_kwargs['Marker'] = bookmark

    page = []
    for s3object in bucket.objects.filter(**filter_kwargs):
        if context.get_remaining_time_in_millis() < 30000:  # 30 seconds
            # If we're not done listing the bucket, then we return here
            # and the State Machine will re-invoke this Lambda
            return event

        page.append(s3object.key)
        if len(page) == 1000:
            queue.send_message(
                MessageBody=json.dumps(page),
            )
            page.clear()

        event['bookmark'] = s3object.key

    # Send remaining keys, if any
    if page:
        queue.send_message(
            MessageBody=json.dumps(page),
        )

    event['all_pages_listed'] = 'TRUE'
    return event


def handler_split_page(event, context):
    """
    This handler receives a page and puts all S3 object keys from the page
    in the S3 Objects SQS Queue.
    """

    s3objects_queue = boto3.resource('sqs').Queue(os.environ['S3_OBJECTS_SQS_QUEUE_URL'])

    for record in event['Records']:
        page = json.loads(record['body'])
        # Since a page can have up to 1000 S3 object keys and SQS only accepts 10 messages to be
        # sent in a single call, we must chunk the page in batches of 10 SQS messages.
        for batch in chunks(page, 10):
            s3objects_queue.send_messages(
                Entries=[
                    {
                        'Id': str(uuid.uuid4()),
                        'MessageBody': key,
                    } for key in batch
                ]
            )


def handler_transform(event, context):
    """
    This handler will receive up to 10 S3 objects keys as input.
    The ETL will be run on each key individually.
    """

    s3 = boto3.resource('s3')

    for record in event['Records']:
        s3object_key = record['body']
        source_s3object = s3.Object(os.environ['SOURCE_BUCKET_NAME'], s3object_key)
        destination_s3object = s3.Object(os.environ['DESTINATION_BUCKET_NAME'], s3object_key)

        lines = list(source_s3object.get()['Body'].read().splitlines())

        # This is where the ELT happens.
        # First, We load the original JSON to a dict. Then, we flatten the dict.
        # Finally, we serialize the flattened dict to JSON.
        items = [json.dumps(flatten(json.loads(line))) for line in lines]

        destination_s3object.put(
            Body='\n'.join(items)
        )

        # Just sending some metrics to CloudWatch
        # so we can monitor the progress of the ETL.
        cloudwatch = boto3.client('cloudwatch')
        cloudwatch.put_metric_data(
            Namespace='custom/lambda-etl',
            MetricData=[
                {
                    'MetricName': 'S3ObjectsProcessed',
                    'Value': 1,
                },
                {
                    'MetricName': 'LinesProcessed',
                    'Value': len(items),
                }
            ],
        )


def flatten(d, parent_key='', sep='.'):
    """
    Takes a dict and returns a flattened representation
    Source: https://stackoverflow.com/a/6027615/902751
    """
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, collections.MutableMapping):
            items.extend(flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


def chunks(l, n):
    """
    Yield successive n-sized chunks from l.
    Source: http://stackoverflow.com/a/312464/902751
    """
    for i in range(0, len(l), n):
        yield l[i:i + n]
