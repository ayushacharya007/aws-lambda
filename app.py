#!/usr/bin/env python3
import os

import aws_cdk as cdk

from stacks.base_stack import BaseStack
from stacks.s3_sqs_lambda_stack import S3SqsLambdaStack
from stacks.s3_sns_lambda_stack import S3SnsLambdaStack
from stacks.event_bridge_lambda_stack import EventBridgeLambdaStack
from stacks.glue_lambda_stack import GlueLambdaStack



app = cdk.App()

# Instantiate BaseStack with shared resources
base_stack = BaseStack(app, "BaseStack")

# Pass base_stack to other stacks
S3SqsLambdaStack(app, "S3SqsLambdaStack", base_stack=base_stack, description="This stack creates an S3 bucket, an SQS queue, and a Lambda function that processes messages from the queue.")
S3SnsLambdaStack(app, "S3SnsLambdaStack", base_stack=base_stack, description="This stack creates an S3 bucket, an SNS topic, and a Lambda function that processes messages from the topic.")
EventBridgeLambdaStack(app, "EventBridgeLambdaStack", base_stack=base_stack, description="This stack creates an EventBridge rule and a Lambda function that processes messages from the rule.")
GlueLambdaStack(app, "GlueLambdaStack", base_stack=base_stack, description="This stack collects row counts from Glue catalogs and stores them in a Glue table for data monitoring.")

app.synth()
