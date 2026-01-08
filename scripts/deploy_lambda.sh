#!/bin/bash
# Deploy Lambda function from Docker image

set -e

# Configuration
AWS_ACCOUNT_ID="${AWS_ACCOUNT_ID:-}"
AWS_REGION="${AWS_REGION:-us-east-2}"
REPO_NAME="vm-dataset-generator"
FUNCTION_NAME="${LAMBDA_FUNCTION_NAME:-vm-dataset-generator}"
OUTPUT_BUCKET="${OUTPUT_BUCKET:-vm-dataset-test}"
MEMORY_SIZE="${LAMBDA_MEMORY:-1024}"
TIMEOUT="${LAMBDA_TIMEOUT:-900}"  # 15 minutes in seconds
ROLE_NAME="${LAMBDA_ROLE_NAME:-vm-dataset-lambda-role}"

# Get AWS Account ID if not set
if [ -z "$AWS_ACCOUNT_ID" ]; then
    echo "Getting AWS Account ID..."
    AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text 2>/dev/null || echo "")
    if [ -z "$AWS_ACCOUNT_ID" ]; then
        echo "Error: Could not determine AWS Account ID. Please set AWS_ACCOUNT_ID environment variable."
        exit 1
    fi
fi

IMAGE_URI="$AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/$REPO_NAME:latest"
ROLE_ARN="arn:aws:iam::${AWS_ACCOUNT_ID}:role/${ROLE_NAME}"

echo "=========================================="
echo "Deploying Lambda Function"
echo "=========================================="
echo "AWS Account ID: $AWS_ACCOUNT_ID"
echo "AWS Region: $AWS_REGION"
echo "Function Name: $FUNCTION_NAME"
echo "Image URI: $IMAGE_URI"
echo "Output Bucket: $OUTPUT_BUCKET"
echo "=========================================="
echo ""

# Step 1: Build and push Docker image
echo "Step 1: Building and pushing Docker image..."
cd "$(dirname "$0")/.."
export AWS_ACCOUNT_ID
export AWS_REGION
./scripts/build_and_push.sh

# Step 2: Create IAM role if it doesn't exist
echo ""
echo "Step 2: Checking IAM role..."
if ! aws iam get-role --role-name "$ROLE_NAME" --region "$AWS_REGION" &>/dev/null; then
    echo "Creating IAM role: $ROLE_NAME"
    
    # Trust policy for Lambda
    cat > /tmp/trust-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "lambda.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF
    
    aws iam create-role \
        --role-name "$ROLE_NAME" \
        --assume-role-policy-document file:///tmp/trust-policy.json \
        --region "$AWS_REGION"
    
    # Attach basic Lambda execution policy
    aws iam attach-role-policy \
        --role-name "$ROLE_NAME" \
        --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole \
        --region "$AWS_REGION"
    
    # Create and attach policy for S3 and SQS access
    cat > /tmp/lambda-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:GetObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::${OUTPUT_BUCKET}",
        "arn:aws:s3:::${OUTPUT_BUCKET}/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "sqs:ReceiveMessage",
        "sqs:DeleteMessage",
        "sqs:GetQueueAttributes"
      ],
      "Resource": "*"
    }
  ]
}
EOF
    
    aws iam put-role-policy \
        --role-name "$ROLE_NAME" \
        --policy-name "S3SQSAccess" \
        --policy-document file:///tmp/lambda-policy.json \
        --region "$AWS_REGION"
    
    echo "Waiting for IAM role to be ready..."
    sleep 10
else
    echo "IAM role already exists: $ROLE_NAME"
fi

# Step 3: Create or update Lambda function
echo ""
echo "Step 3: Creating/updating Lambda function..."

if aws lambda get-function --function-name "$FUNCTION_NAME" --region "$AWS_REGION" &>/dev/null; then
    echo "Updating existing Lambda function..."
    aws lambda update-function-code \
        --function-name "$FUNCTION_NAME" \
        --image-uri "$IMAGE_URI" \
        --region "$AWS_REGION" \
        > /dev/null
    
    echo "Waiting for function update to complete..."
    aws lambda wait function-updated --function-name "$FUNCTION_NAME" --region "$AWS_REGION"
    
    # Update configuration (AWS_REGION is automatically set by Lambda)
    aws lambda update-function-configuration \
        --function-name "$FUNCTION_NAME" \
        --memory-size "$MEMORY_SIZE" \
        --timeout "$TIMEOUT" \
        --environment "Variables={OUTPUT_BUCKET=${OUTPUT_BUCKET}}" \
        --region "$AWS_REGION" \
        > /dev/null
    
    echo "Function updated successfully!"
else
    echo "Creating new Lambda function..."
    aws lambda create-function \
        --function-name "$FUNCTION_NAME" \
        --package-type Image \
        --code ImageUri="$IMAGE_URI" \
        --role "$ROLE_ARN" \
        --memory-size "$MEMORY_SIZE" \
        --timeout "$TIMEOUT" \
        --environment "Variables={OUTPUT_BUCKET=${OUTPUT_BUCKET}}" \
        --architectures x86_64 \
        --region "$AWS_REGION"
    
    echo "Waiting for function to be active..."
    aws lambda wait function-active --function-name "$FUNCTION_NAME" --region "$AWS_REGION"
    
    echo "Function created successfully!"
fi

echo ""
echo "=========================================="
echo "Deployment Complete!"
echo "=========================================="
echo "Function Name: $FUNCTION_NAME"
echo "Function ARN: $(aws lambda get-function --function-name "$FUNCTION_NAME" --region "$AWS_REGION" --query 'Configuration.FunctionArn' --output text)"
echo ""
echo "To connect SQS trigger, run:"
echo "  aws lambda create-event-source-mapping \\"
echo "    --function-name $FUNCTION_NAME \\"
echo "    --event-source-arn <SQS_QUEUE_ARN> \\"
echo "    --batch-size 1 \\"
echo "    --region $AWS_REGION"

