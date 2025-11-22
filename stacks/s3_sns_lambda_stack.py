from aws_cdk import (
    Stack,
    Duration,
    RemovalPolicy,
    aws_s3 as s3,
    aws_s3_deployment as s3_deploy,
    aws_lambda as _lambda,
    # aws_lambda_event_sources as lambda_event_source, # optional import if using event source
    aws_sns as sns,
    aws_sns_subscriptions as sns_subs,
    aws_s3_notifications as s3n,
    aws_sqs as sqs,
)
import aws_cdk as cdk
from constructs import Construct
import os
from dotenv import load_dotenv
load_dotenv(dotenv_path="../.env")

class S3SnsLambdaStack(Stack):
    """CDK stack that wires S3 -> SNS -> Lambda for object processing.

    - Creates an S3 bucket to store raw data.
    - Creates an SNS topic to receive S3 notifications.
    - Creates a Lambda function (with awswrangler layer) that consumes messages
      from the SNS topic and has read/write access to the bucket.
    """

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create an SNS topic used to buffer S3 notifications for the Lambda.
        topic = sns.Topic(self, "DemoTopic",
                          topic_name="s3-sns-lambda-topic",
                          )
        
        # Create a dead-letter queue for failed Lambda invocations.
        dlq = sqs.Queue(self, "DemoDeadLetterQueue",
                        queue_name="s3-sns-lambda-dlq",
                        removal_policy=RemovalPolicy.DESTROY
                        )

        # Create the S3 bucket that will trigger notifications.
        bucket = s3.Bucket(self, "MyTestBucket",
                           bucket_name=f's3-sns-test-{cdk.Aws.ACCOUNT_ID}-{cdk.Aws.REGION}',
                           versioned=False,
                           encryption=s3.BucketEncryption.S3_MANAGED,
                           removal_policy=RemovalPolicy.DESTROY,
                           auto_delete_objects=True,
                           block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
                           enforce_ssl=True
                           )
        
        # Configure the bucket to send notifications to the SNS topic
        bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.SnsDestination(topic=topic),  # type: ignore
            s3.NotificationKeyFilter(prefix="Raw/", suffix=".csv")
        )

        # # Deploy local resources into the bucket at deploy time.
        s3_deploy.BucketDeployment(self, "RawSources",
                                   sources=[s3_deploy.Source.asset("../resources")],
                                   destination_bucket=bucket,
                                   destination_key_prefix="Raw"
                                   )
        
        # AWS Wrangler Layer
        wrangler_layer = _lambda.LayerVersion.from_layer_version_arn(self, "SharedAwsWranglerLayer",
            layer_version_arn=os.environ["LAMBDA_LAYER_ARN"]
        )   

        # Create the Lambda function that will process SNS notifications.
        fn = _lambda.Function(self, "FileTransformSNSLambdaFunction",
                              runtime=_lambda.Runtime.PYTHON_3_13,
                              handler="sns_lambda.handler",
                              code=_lambda.Code.from_asset("src/lambdas"),
                              layers=[wrangler_layer],
                              architecture=_lambda.Architecture.ARM_64,
                              timeout=Duration.seconds(300),
                              memory_size=512,
                              )

        # Subscribe the Lambda function to the SNS topic with a DLQ.
        topic.add_subscription(sns_subs.LambdaSubscription(fn, dead_letter_queue=dlq)) # type: ignore

        # Or alternatively, add the event source directly to the Lambda function.
        # Uncomment the following line and comment out the above subscription to use this method.
        
        # fn.add_event_source(lambda_event_source.SnsEventSource(cast(sns.ITopic, topic), dead_letter_queue=dlq))
        
        # Grant the Lambda function read/write permissions to the bucket.
        bucket.grant_read_write(fn)
        