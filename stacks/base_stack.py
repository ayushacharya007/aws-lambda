from aws_cdk import (
    Stack,
    RemovalPolicy,
    aws_s3 as s3,
    aws_glue as glue,
    aws_lambda as _lambda,
)
import aws_cdk as cdk
from constructs import Construct
import os
from dotenv import load_dotenv
# Load .env from the project root (one level up from stacks/)
dotenv_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')
load_dotenv(dotenv_path)

class BaseStack(Stack):
    """
    Base Stack that defines shared resources used by other stacks.
    
    Resources:
    - Shared S3 Bucket
    - Shared Glue Database
    - Shared AWS Wrangler Lambda Layer
    """

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        
        # Shared S3 Glue Result Bucket
        self.glue_result_bucket = s3.Bucket(self, "GlueBucket",
                            bucket_name=f's3-glue-test-{cdk.Aws.ACCOUNT_ID}-{cdk.Aws.REGION}',
                            versioned=False,
                            encryption=s3.BucketEncryption.S3_MANAGED,
                            removal_policy=RemovalPolicy.DESTROY,
                            auto_delete_objects=True,
                            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
                            enforce_ssl=True
                            )
        

        # Shared Glue Database
        self.glue_database = glue.CfnDatabase(self, "GlueDatabase",
            catalog_id=cdk.Aws.ACCOUNT_ID,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name="data_monitoring_db",
                description="Database for data monitoring and quality checks.",
            )
        )

        # Shared Lambda Layer (AWS Wrangler) (Use from .env)
        self.wrangler_layer = _lambda.LayerVersion.from_layer_version_arn(self, "SharedAwsWranglerLayer",
            layer_version_arn=os.environ["LAMBDA_LAYER_ARN"]
        )   
