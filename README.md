# lambda-sqs-etl
Serverless ETL pipeline using Lambda and SQS

# Deployment
```
aws cloudformation package --template-file cloudformation-stack.yml --s3-bucket YOUR_CLOUDFORMATINO_BUCKET --output-template-file packaged.yml && aws cloudformation deploy --template-file packaged.yml --stack-name SOME_STACK_NAME --capabilities CAPABILITY_IAM --parameter-overrides SourceBucket=YOUR_SOURCE_BUCKET DestinationBucket=YOUR_DESTINATION_BUCKET
```

# Running the pipeline
Simply start an execution of the state machine that was created by the CloudFormation template
