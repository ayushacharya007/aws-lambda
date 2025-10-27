from aws_cdk import (
    Stack,
    Duration,
    RemovalPolicy,
    aws_s3 as s3,
    aws_s3_deployment as s3_deploy,
    aws_lambda as _lambda,
    aws_events as events,
    aws_events_targets as targets,
    aws_sqs as sqs,
    # aws_iam as iam,
)
from constructs import Construct
from typing import cast
import os


class EventBridgeLambdaStack(Stack):
    """CDK stack that wires S3 -> EventBridge -> Lambda for object processing.
    
    - Creates an S3 bucket with server-side encryption, versioning and
      automatic object deletion on stack destroy (for dev/test).
    - Creates an EventBridge bus to receive S3 notifications.
    - Creates a Lambda function (with awswrangler layer) that consumes messages
      from the EventBridge bus and has read/write access to the bucket.
    """
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create the S3 bucket that will trigger notifications.
        source_bucket = s3.Bucket(self, "SourceBucket",
                           bucket_name=f's3-event-bridge-source-{os.getenv("BUCKET_NAME")}',
                           versioned=False,
                           encryption=s3.BucketEncryption.S3_MANAGED,
                           removal_policy=RemovalPolicy.DESTROY,
                           auto_delete_objects=True,
                           block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
                           enforce_ssl=True,
                           event_bridge_enabled=True
                           )
        
        # Create a destination bucket to store processed files
        destination_bucket = s3.Bucket(self, "ProcessedBucket",
                           bucket_name=f's3-event-bridge-processed-{os.getenv("BUCKET_NAME")}',
                           versioned=False,
                           encryption=s3.BucketEncryption.S3_MANAGED,
                           removal_policy=RemovalPolicy.DESTROY,
                           auto_delete_objects=True,
                           block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
                           enforce_ssl=True
                           )
        
        # Create a rule to capture S3 Object Created events and route them to the EventBridge bus
        rule = events.Rule(self, "S3EventRule",
                           event_pattern=events.EventPattern(
                               source=["aws.s3"],
                               detail_type=["Object Created"],
                                detail={
                                    "bucket": {
                                        "name": [source_bucket.bucket_name],
                                    },
                                    "object": {
                                        "key": [{
                                            "wildcard": "Raw/*.csv" # match any CSV file in the 'Raw/' folder
                                        }]
                                    }
                                }
                           ),
                           rule_name="S3ObjectCreatedRule"
                           )

        # Lambda layer providing awswrangler (or other libraries) to the function.
        wrangler_layer = _lambda.LayerVersion.from_layer_version_arn(self, "AwsWranglerLayer",
                                                                     layer_version_arn="arn:aws:lambda:ap-southeast-2:336392948345:layer:AWSSDKPandas-Python313-Arm64:4"
                                                                     )

        # Create the Lambda function that will process SNS notifications.
        fn = _lambda.Function(self, "EventBridgeTransformLambda",
                              function_name="event-bridge-transform-lambda",
                              runtime=_lambda.Runtime.PYTHON_3_13,
                              handler="event_bridge_lambda.handler",
                              code=_lambda.Code.from_asset("src/lambdas"),
                              layers=[wrangler_layer],
                              architecture=_lambda.Architecture.ARM_64,
                              timeout=Duration.seconds(300),
                              memory_size=512,
                              environment={
                                    "DESTINATION_BUCKET_NAME": f's3-event-bridge-processed-{os.getenv("BUCKET_NAME")}'
                                }
                              )
        
        # Create a Dead Letter Queue for failed event processing
        dlq = sqs.Queue(self, "EventDLQ",
                        queue_name="EventDLQ",
                        removal_policy=RemovalPolicy.DESTROY
                        )  
        
        # Add Lambda function as target with proper type handling
        lambda_target = targets.LambdaFunction(
            handler=cast(_lambda.IFunction, fn),
            dead_letter_queue=dlq,
            max_event_age=Duration.hours(1),
            retry_attempts=2
        )
        rule.add_target(cast(events.IRuleTarget, lambda_target))

        # Grant the Lambda function read/write permissions on the S3 buckets.
        source_bucket.grant_read(fn)
        destination_bucket.grant_read_write(fn)

        # # Deploy local resources into the bucket at deploy time.
        # s3_deploy.BucketDeployment(self, "RawSources",
        #                            sources=[s3_deploy.Source.asset("../resources")],
        #                            destination_bucket=source_bucket,
        #                            destination_key_prefix="Raw"
        #                            )