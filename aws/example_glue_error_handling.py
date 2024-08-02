import json
import boto3
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    glue = boto3.client('glue')
    
    try:
        # Extract bucket name and file key from the event
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        file_key = event['Records'][0]['s3']['object']['key']
        
        # Log the details
        logger.info(f"Processing file: s3://{bucket_name}/{file_key}")
        
        # Trigger AWS Glue Job for ETL processing
        response = glue.start_job_run(
            JobName='my-glue-job',
            Arguments={
                '--bucket_name': bucket_name,
                '--file_key': file_key
            }
        )
        
        logger.info(f"Started Glue job: {response['JobRunId']}")
    
    except Exception as e:
        logger.error(f"Error processing file: {str(e)}")
        raise e
    
    return {
        'statusCode': 200,
        'body': json.dumps('ETL job started')
    }