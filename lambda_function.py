import json
import boto3
import os
import time

sm = boto3.client("sagemaker")

def lambda_handler(event, context):
    # Environment variables
    bucket = os.environ["BUCKET"]
    image_uri = os.environ["ECR_IMAGE"]
    role = os.environ["EXEC_ROLE"]
    master_key = os.environ["MASTER_KEY"]

    # Uploaded file key
    record = event["Records"][0]
    new_file = record["s3"]["object"]["key"]

    # Job name initialisation
    job_name = f"inv-forecast-{int(time.time())}"
    print(f"Triggered by: s3://{bucket}/{new_file}")
    print(f"Job name: {job_name}")

    response = sm.create_processing_job(
        ProcessingJobName=job_name,
        RoleArn=role,
        AppSpecification={
            "ImageUri": image_uri,
            "ContainerEntrypoint": ["python", "/opt/ml/processing/code/processing_script2.py"]
        },
        ProcessingInputs=[
            {
                "InputName": "new-file",
                "S3Input": {
                    "S3Uri": f"s3://{bucket}/{new_file}",
                    "LocalPath": "/opt/ml/processing/input",
                    "S3DataType": "S3Prefix",
                    "S3InputMode": "File"
                }
            },
            {
                "InputName": "ledger",
                "S3Input": {
                    "S3Uri": f"s3://{bucket}/Historic/combined_ledger.csv",
                    "LocalPath": "/opt/ml/processing/historic",
                    "S3DataType": "S3Prefix",
                    "S3InputMode": "File"
                }
            },
            {
                "InputName": "forecast",
                "S3Input": {
                    "S3Uri": f"s3://{bucket}/{master_key}",
                    "LocalPath": "/opt/ml/processing/forecast",
                    "S3DataType": "S3Prefix",
                    "S3InputMode": "File"
                }
            }
        ],
        ProcessingOutputConfig={
            "Outputs": [
                {
                    "OutputName": "ledger-out",
                    "S3Output": {
                        "S3Uri": f"s3://{bucket}/Historic/",
                        "LocalPath": "/opt/ml/processing/output/historic",
                        "S3UploadMode": "EndOfJob"
                    }
                },
                {
                    "OutputName": "forecast-out",
                    "S3Output": {
                        "S3Uri": f"s3://{bucket}/Output/",
                        "LocalPath": "/opt/ml/processing/output/forecast",
                        "S3UploadMode": "EndOfJob"
                    }
                }
            ]
        },
        ProcessingResources={
            "ClusterConfig": {
                "InstanceCount": 1,
                "InstanceType": "ml.m5.12xlarge",
                "VolumeSizeInGB": 30
            }
        },
        Environment={
            "BUCKET": bucket,
            "NEW_FILE": new_file,
            "MASTER_FILE": master_key
        }
    )

    print("SageMaker processing start:", job_name)
    return {
        "statusCode": 200,
        "body": json.dumps(f"Processing job {job_name} started")
    }