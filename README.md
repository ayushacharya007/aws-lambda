# AWS CDK S3 Lambda Project

This project demonstrates various patterns for processing S3 events using AWS Lambda, orchestrated via AWS CDK (Cloud Development Kit) in Python. It showcases integrations with SQS, SNS, EventBridge, and AWS Glue.

## Architecture Overview

The project consists of independent feature stacks that demonstrate specific AWS integration patterns. Each stack creates its own necessary resources (S3 buckets, queues, topics) to function autonomously, while referencing shared configuration (like Lambda Layers) via environment variables.

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
    -   **Resources**: Dedicated S3 Bucket, Glue Database, Lambda.

## Prerequisites

-   **Python**: 3.12 or later.
-   **Node.js**: Version 22 (Compatible with AWS CDK).
-   **AWS CDK CLI**: Installed globally (`npm install -g aws-cdk`).
-   **AWS CLI**: Configured with appropriate credentials.
-   **uv**: For dependency management (recommended).

## Setup & Installation

1.  **Clone the repository**:
    ```bash
    git clone <repository-url>
    cd s3_lambda
    ```

2.  **Environment Setup**:
    This project is part of a workspace managed by `uv`.
    ```bash
    # From the root of the repo
    uv sync
    source .venv/bin/activate
    ```

3.  **Configure Environment Variables**:
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
    cdk deploy S3SqsLambdaStack
    cdk deploy S3SnsLambdaStack
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
    -   `s3_sqs_lambda_stack.py`: S3-SQS pattern.
    -   `s3_sns_lambda_stack.py`: S3-SNS pattern.
    -   `event_bridge_lambda_stack.py`: EventBridge pattern.
    -   `data_scheme_stack.py`: Glue integration.
-   `src/lambdas/`: Python source code for Lambda functions.
