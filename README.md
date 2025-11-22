# AWS CDK S3 Lambda Project

This project demonstrates various patterns for processing S3 events using AWS Lambda, orchestrated via AWS CDK (Cloud Development Kit) in Python. It showcases integrations with SQS, SNS, EventBridge, and AWS Glue.

## Architecture Overview

The project is structured around a **BaseStack** that provides shared resources to other feature-specific stacks. This promotes resource reuse and cleaner architecture.

### Shared Resources (`BaseStack`)
- **Glue Database**: A shared Glue Catalog Database (`shared_data_monitoring_db`) for data monitoring.
- **Glue Result Bucket**: A shared S3 bucket for storing query results and Glue data.
- **Lambda Layer**: A shared AWS Wrangler (Pandas) Lambda Layer for data processing capabilities.

### Feature Stacks
1.  **S3SqsLambdaStack**:
    -   **Flow**: S3 Upload -> SQS Queue -> Lambda Function.
    -   **Use Case**: Buffered processing of file uploads.
    -   **Resources**: Dedicated S3 Bucket, SQS Queue, DLQ, Lambda.

2.  **S3SnsLambdaStack**:
    -   **Flow**: S3 Upload -> SNS Topic -> Lambda Function.
    -   **Use Case**: Fan-out architecture or immediate notification processing.
    -   **Resources**: Dedicated S3 Bucket, SNS Topic, Lambda.

3.  **EventBridgeLambdaStack**:
    -   **Flow**: S3 Upload -> EventBridge Rule -> Lambda Function.
    -   **Use Case**: Event-driven architecture with complex filtering rules.
    -   **Resources**: Dedicated S3 Bucket (Source & Destination), EventBridge Rule, Lambda.

4.  **GlueLambdaStack**:
    -   **Flow**: Scheduled Event / Direct Invoke -> Lambda -> AWS Glue/Athena.
    -   **Use Case**: Data quality checks, schema monitoring, and running Athena queries.
    -   **Resources**: Uses shared Glue DB and Result Bucket.

## Prerequisites

-   **Python**: 3.13 or later.
-   **Node.js**: Version 22 (Compatible with AWS CDK).
-   **AWS CDK CLI**: Installed globally (`npm install -g aws-cdk`).
-   **AWS CLI**: Configured with appropriate credentials.

## Setup & Installation

1.  **Clone the repository**:
    ```bash
    git clone <repository-url>
    cd s3_lambda
    ```

2.  **Create and activate a virtual environment**:
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    ```

3.  **Install dependencies**:
    ```bash
    pip install -r requirements.txt
    ```

4.  **Configure Environment Variables**:
    Create a `.env` file in the project root with the following variables:
    ```ini
    ACCOUNT_ID=<your-aws-account-id>
    REGION=<your-aws-region>
    LAMBDA_LAYER_ARN=<arn-of-aws-wrangler-layer>
    ```

## Deployment

1.  **Synthesize the CloudFormation templates**:
    ```bash
    cdk synth
    ```

2.  **Deploy all stacks**:
    ```bash
    cdk deploy --all
    ```
    Or deploy specific stacks:
    ```bash
    cdk deploy BaseStack
    cdk deploy S3LambdaStack
    ```

## Useful Commands

-   `cdk ls`: List all stacks in the app.
-   `cdk synth`: Emits the synthesized CloudFormation template.
-   `cdk deploy`: Deploy this stack to your default AWS account/region.
-   `cdk diff`: Compare deployed stack with current state.
-   `cdk docs`: Open CDK documentation.

## Project Structure

-   `app.py`: Entry point of the CDK application.
-   `stacks/`: Contains CDK stack definitions.
    -   `base_stack.py`: Shared resources.
    -   `s3_sqs_lambda_stack.py`: S3-SQS pattern.
    -   `s3_sns_lambda_stack.py`: S3-SNS pattern.
    -   `event_bridge_lambda_stack.py`: EventBridge pattern.
    -   `glue_lambda_stack.py`: Glue integration.
-   `src/lambdas/`: Python source code for Lambda functions.
