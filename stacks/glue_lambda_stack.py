from aws_cdk import (
    Stack,
    Duration,
    RemovalPolicy,
    aws_lambda as _lambda,
    aws_glue as glue,
    aws_glue_alpha as glue_alpha,
    aws_s3 as s3,
    aws_iam as iam,
)
from constructs import Construct
from dotenv import load_dotenv
import os

load_dotenv(dotenv_path="../.env")  

class GlueLambdaStack(Stack):
    """CDK stack that creates a Lambda function to interact with AWS Glue.

    - Creates a Lambda function (with awswrangler layer) that can interact
      with AWS Glue.
    """

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        
        # Create an S3 bucket to store Glue-related data
        glue_result_bucket = s3.Bucket(self, "GlueBucket",
                            bucket_name=f's3-glue-test-{os.getenv("BUCKET_NAME")}',
                            versioned=False,
                            encryption=s3.BucketEncryption.S3_MANAGED,
                            removal_policy=RemovalPolicy.DESTROY,
                            auto_delete_objects=True,
                            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
                            enforce_ssl=True
                            )
        
        # Create glue database
        # glue_database = glue.CfnDatabase(self, "GlueDatabase",
        #                             catalog_id=os.getenv("AWS_ACCOUNT_ID", " "),
        #                             database_input=glue.CfnDatabase.DatabaseInputProperty(
        #                                 name="data-monitoring-database",
        #                                 description="Database for analysts to monitor data quality and schema changes.",
        #                             )
        #                         )
        glue_database = glue_alpha.Database(self, "GlueDatabase",
                                            database_name="data-monitoring-database",
                                            description="Database to store data monitoring results."
                                            )   
        # Create Glue Table to store the results
        # glue_table = glue.CfnTable(self, "GlueTable",
        #                            catalog_id=os.getenv("ACCOUNT_ID", " "),
        #                            database_name="data-monitoring-database",
        #                            table_input=glue.CfnTable.TableInputProperty(
        #                                name="data_monitoring_table",
        #                                description="Table to store row counts per database table.",
        #                                table_type="EXTERNAL_TABLE",
        #                                parameters={
        #                                    "classification": "csv",
        #                                    "skip.header.line.count": "1"
        #                                 },
        #                                 storage_descriptor=glue.CfnTable.StorageDescriptorProperty(
        #                                     columns=[
        #                                         glue.CfnTable.ColumnProperty(name="database_name", type="string"),
        #                                         glue.CfnTable.ColumnProperty(name="table_name", type="string"),
        #                                         glue.CfnTable.ColumnProperty(name="row_count", type="bigint"),
        #                                         glue.CfnTable.ColumnProperty(name="last_updated", type="timestamp"),
        #                                     ],
        #                                     location=f's3://{result_bucket.bucket_name}/data-schema/',
        #                                     input_format="org.apache.hadoop.mapred.TextInputFormat",
        #                                     output_format="org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
        #                                     serde_info=glue.CfnTable.SerdeInfoProperty(
        #                                         serialization_library="org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
        #                                         parameters={"field.delim": ","}
        #                                     )
        #                                 ),
        #                            )
        #                           )
        
        glue_table = glue_alpha.S3Table(self, "GlueTable",
                                        database=glue_database,
                                        table_name="data_monitoring_table",
                                        description="Table to store row counts per database table.",
                                        bucket=glue_result_bucket,
                                        s3_prefix="data-schema/",       
                                        columns=[
                                            glue_alpha.Column(name="database_name", type=glue_alpha.Schema.STRING),
                                            glue_alpha.Column(name="table_name", type=glue_alpha.Schema.STRING),
                                            glue_alpha.Column(name="row_count", type=glue_alpha.Schema.BIG_INT),
                                            glue_alpha.Column(name="last_updated", type=glue_alpha.Schema.TIMESTAMP),
                                        ],
                                        data_format=glue_alpha.DataFormat.PARQUET,
                                        )
        
        # Ensure the table is created after the database
        # glue_table.add_dependency(glue_database)
        glue_table.database.node.add_dependency(glue_database)

        # Lambda layer providing awswrangler (or other libraries) to the function.
        wrangler_layer = _lambda.LayerVersion.from_layer_version_arn(self, "AwsWranglerLayer",
                                                                     layer_version_arn="arn:aws:lambda:ap-southeast-2:336392948345:layer:AWSSDKPandas-Python313-Arm64:4"
                                                                     )
        # Create the Lambda function that interacts with AWS Glue.
        glue_lambda = _lambda.DockerImageFunction(self, "GlueLambdaFunction",
                                                    function_name="GlueInteractionLambda",
                                                    code=_lambda.DockerImageCode.from_image_asset("src"),
                                                    timeout=Duration.seconds(300),
                                                    memory_size=512,
                                                    architecture=_lambda.Architecture.ARM_64,
                                                    )
        
        # Grant the Lambda function read access to AWS Glue
        glue_lambda.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "glue:GetDatabase",
                    "glue:GetTable",
                    "glue:GetTables",
                    "glue:GetDatabases",
                ],
                effect=iam.Effect.ALLOW,
                resources=[
                    f"arn:aws:glue:{os.getenv('REGION')}:{os.getenv('ACCOUNT_ID')}:catalog",
                    f"arn:aws:glue:{os.getenv('REGION')}:{os.getenv('ACCOUNT_ID')}:database/*",
                    f"arn:aws:glue:{os.getenv('REGION')}:{os.getenv('ACCOUNT_ID')}:table/*/*",
                ]
            )
        )

        # Grant the Lambda function permissions to create/update/delete tables and databases in AWS Glue
        glue_lambda.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "glue:CreateTable",
                    "glue:UpdateTable",
                    "glue:DeleteTable",
                    "glue:CreateDatabase",
                    "glue:UpdateDatabase",
                    "glue:DeleteDatabase",
                ],
                effect=iam.Effect.ALLOW,
                resources=[
                    f"arn:aws:glue:{os.getenv('REGION')}:{os.getenv('ACCOUNT_ID')}:catalog",
                    f"arn:aws:glue:{os.getenv('REGION')}:{os.getenv('ACCOUNT_ID')}:database/{glue_database.ref}",
                    f"arn:aws:glue:{os.getenv('REGION')}:{os.getenv('ACCOUNT_ID')}:table/{glue_database.ref}/data_monitoring_table",
                ]
            )
        )
        
        # Grant the Lambda function read/write access to the S3 bucket
        glue_result_bucket.grant_read_write(glue_lambda)