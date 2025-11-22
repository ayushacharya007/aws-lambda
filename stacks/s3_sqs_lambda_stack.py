from aws_cdk import (
    Stack,
    Duration,
    RemovalPolicy,
    aws_s3 as s3,
    aws_s3_deployment as s3_deploy,
    aws_s3_notifications as s3n,
    aws_lambda as _lambda,
    aws_lambda_event_sources as lambda_event_source,
    aws_sqs as sqs,
)
from constructs import Construct
import aws_cdk as cdk
import os
from dotenv import load_dotenv
load_dotenv(dotenv_path="../.env")

class S3SqsLambdaStack(Stack):
    """CDK stack that wires S3 -> SQS -> Lambda for object processing.

    - Creates an S3 bucket to store raw data.
    - Creates an SQS queue to receive S3 notifications.
    - Creates a Lambda function (with awswrangler layer) that consumes messages
      from the queue and has read/write access to the bucket.
    """

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create a dead letter queue for failed messages
        dlq = sqs.Queue(self, "DemoDeadLetterQueue",
                        queue_name="s3-sqs-lambda-dlq",
                        removal_policy=RemovalPolicy.DESTROY
                        )

        # Create an SQS queue used to buffer S3 notifications for the Lambda.
        queue = sqs.Queue(self, "DemoQueue",
                          queue_name="s3-sqs-lambda-queue",
                          visibility_timeout=Duration.seconds(500),
                          dead_letter_queue=sqs.DeadLetterQueue(
                              max_receive_count=1,
                              queue=dlq
                          ),
                          removal_policy=RemovalPolicy.DESTROY,
                          )

        # Create the S3 bucket that will trigger notifications.
        bucket = s3.Bucket(self, "MyTestBucket",
                           bucket_name=f's3-sqs-test-{cdk.Aws.ACCOUNT_ID}-{cdk.Aws.REGION}',
                           encryption=s3.BucketEncryption.S3_MANAGED,
                           removal_policy=RemovalPolicy.DESTROY,
                           auto_delete_objects=True,
                           block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
                           enforce_ssl=True,
                           )
        
        # Configure the bucket to send notifications to the SQS queue
        bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.SqsDestination(queue=queue), # type: ignore
            s3.NotificationKeyFilter(prefix="Raw/", suffix=".csv")
        )

        # Deploy local resources into the bucket at deploy time.
        s3_deploy.BucketDeployment(self, "RawSources",
                                   sources=[s3_deploy.Source.asset("../resources")],
                                   destination_bucket=bucket,
                                   destination_key_prefix="Raw"
                                   )

        # AWS Wrangler Layer
        wrangler_layer = _lambda.LayerVersion.from_layer_version_arn(self, "SharedAwsWranglerLayer",
            layer_version_arn=os.environ["LAMBDA_LAYER_ARN"]
        )   

        # Lambda function that will process S3 object events.
        transform_fn = _lambda.Function(self, "FileTransformLambda",
                                        runtime=_lambda.Runtime.PYTHON_3_13,
                                        handler="sqs_lambda.handler",
                                        code=_lambda.Code.from_asset("src/lambdas"),
                                        layers=[wrangler_layer],
                                        architecture=_lambda.Architecture.ARM_64,
                                        timeout=Duration.seconds(500),
                                        memory_size=512,
                                        )

        # Allow the Lambda to consume messages from the queue (Receive/Delete/ChangeVisibility).
        queue.grant_consume_messages(transform_fn)
        
        # Grant the Lambda function read/write permissions to the bucket.
        bucket.grant_read_write(transform_fn)
        
        # Configure the Lambda to be triggered by messages in the SQS queue.
        transform_fn.add_event_source(
            lambda_event_source.SqsEventSource(
                queue=queue,
                batch_size=1
            )
        )