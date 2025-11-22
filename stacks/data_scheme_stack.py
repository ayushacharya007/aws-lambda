from aws_cdk import (
    Stack,
    Duration,
    RemovalPolicy,
    aws_lambda as _lambda,
    aws_glue as glue,
    aws_s3 as s3,
    aws_iam as iam,
)
import aws_cdk as cdk
from constructs import Construct
from .base_stack import BaseStack

class GlueLambdaStack(Stack):
    """CDK stack that creates a Lambda function to interact with AWS Glue.

    - Creates a Lambda function (with shared awswrangler layer) that can interact
      with AWS Glue.
    - Uses shared Glue Database and S3 Bucket from BaseStack.
    """

    def __init__(self, scope: Construct, construct_id: str, base_stack: BaseStack, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        
        # Use Shared Glue Result Bucket from BaseStack
        result_bucket = base_stack.glue_result_bucket
        
        # Use Shared Glue Database from BaseStack
        glue_database = base_stack.glue_database

        # Use Shared Wrangler Layer from BaseStack
        wrangler_layer = base_stack.wrangler_layer

        # Create the Lambda function that interacts with AWS Glue.
        glue_lambda = _lambda.Function(self, "GlueLambdaFunction",
                                       function_name="GlueLambdaFunction",
                                       runtime=_lambda.Runtime.PYTHON_3_13,
                                       handler="glue_lambda.handler",
                                       code=_lambda.Code.from_asset("src/lambdas"),
                                       timeout=Duration.minutes(15),
                                       memory_size=1024,
                                       layers=[wrangler_layer],
                                       environment={
                                           "BUCKET_NAME": result_bucket.bucket_name,
                                           "GLUE_DATABASE_NAME": glue_database.ref,
                                           "REGION": cdk.Aws.REGION,
                                           "ACCOUNT_ID": cdk.Aws.ACCOUNT_ID,
                                       },
                                        architecture=_lambda.Architecture.ARM_64
                                       )
        
        # Grant the Lambda function read access to AWS Glue
        glue_lambda.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "glue:GetDatabase",
                    "glue:GetDatabases",
                    "glue:GetTable",
                    "glue:GetTables",
                    "glue:CreateDatabase",
                    "glue:UpdateDatabase",
                    "glue:CreateTable",
                    "glue:DeleteTable",
                    "glue:UpdateTable",
                    "glue:GetTableVersions",
                    "glue:GetPartition",
                    "glue:CreatePartition",
                ],
                effect=iam.Effect.ALLOW,
                resources=[
                    f"arn:aws:glue:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:catalog",
                    f"arn:aws:glue:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:database/*",
                    f"arn:aws:glue:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:table/*/*",
                ]
            )
        )
        # Grant athena query permissions to the Lambda function
        glue_lambda.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "athena:StartQueryExecution",
                    "athena:StopQueryExecution",
                    "athena:GetQueryExecution",
                    "athena:GetQueryResults",
                    "athena:ListDatabases",
                    "athena:ListTableMetadata",
                    "athena:ListWorkGroups",
                    "athena:GetWorkGroup",
                    "athena:GetDataCatalog",
                    "athena:ListQueryExecutions"
                ],
                resources=[
                    f"arn:aws:athena:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:workgroup/*"
                ]
            )
        )
        
        # Grant S3 permissions for Athena query results and all Glue data locations
        glue_lambda.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "s3:GetObject",
                    "s3:PutObject",
                    "s3:ListBucket",
                    "s3:GetBucketLocation"
                ],
                resources=[
                    "arn:aws:s3:::*",
                    "arn:aws:s3:::*/*"
                ]
            )
        )
        
        # Grant the Lambda function read/write access to the result bucket
        result_bucket.grant_read_write(glue_lambda)

        # Add a event rule to trigger the Lambda function every day at midnight UTC
        rule = cdk.aws_events.Rule(self, "DailyGlueLambdaTrigger",
                                   schedule=cdk.aws_events.Schedule.cron(minute="0", hour="0"),
                                   targets=[cdk.aws_events_targets.LambdaFunction(glue_lambda)] # type: ignore
                                   )

        
        # add permission for event rule to invoke the lambda function
        glue_lambda.add_permission("AllowEventRuleInvoke",
                                  principal=iam.ServicePrincipal("events.amazonaws.com"), # type: ignore
                                  action="lambda:InvokeFunction",
                                  source_arn=rule.rule_arn
                                  )