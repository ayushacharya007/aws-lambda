#!/usr/bin/env python3
import os

import aws_cdk as cdk

from stacks.s3_sqs_lambda_stack import S3SqsLambdaStack
from stacks.s3_sns_lambda_stack import S3SnsLambdaStack
from stacks.event_bridge_lambda_stack import EventBridgeLambdaStack
from stacks.glue_lambda_stack import GlueLambdaStack



app = cdk.App()

S3SqsLambdaStack(app, "S3LambdaStack")
S3SnsLambdaStack(app, "S3SnsLambdaStack")
EventBridgeLambdaStack(app, "EventBridgeLambdaStack")
GlueLambdaStack(app, "GlueLambdaStack", description="This stack collects row counts from Glue catalogs and stores them in a Glue table for data monitoring.")

app.synth()
