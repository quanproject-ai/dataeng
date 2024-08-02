import boto3
import os

# Initialize S3 client
s3 = boto3.client('s3')

# Function to download file from S3
def download_file_from_s3(bucket_name, object_key, download_path):
    s3.download_file(bucket_name, object_key, download_path)

# Function to upload file to S3
def upload_file_to_s3(bucket_name, object_key, file_path):
    s3.upload_file(file_path, bucket_name, object_key)

# Function to transform the data (convert to uppercase in this example)
def transform_data(input_path, output_path):
    with open(input_path, 'r') as infile, open(output_path, 'w') as outfile:
        for line in infile:
            outfile.write(line.upper())

# Main ETL process
def main():
    source_bucket_name = 'company-data-source'
    source_file_key = 'reports/weekly_report.txt'
    destination_bucket_name = 'company-data-destination'
    destination_file_key = 'processed_reports/weekly_report_transformed.txt'

    # Temporary file paths
    local_source_path = '/tmp/weekly_report.txt'
    local_transformed_path = '/tmp/weekly_report_transformed.txt'

    try:
        # Step 1: Download the file from the source S3 bucket
        download_file_from_s3(source_bucket_name, source_file_key, local_source_path)
        print(f"Downloaded {source_file_key} from {source_bucket_name} to {local_source_path}")

        # Step 2: Transform the data
        transform_data(local_source_path, local_transformed_path)
        print(f"Transformed data and saved to {local_transformed_path}")

        # Step 3: Upload the transformed file to the destination S3 bucket
        upload_file_to_s3(destination_bucket_name, destination_file_key, local_transformed_path)
        print(f"Uploaded transformed file to {destination_bucket_name}/{destination_file_key}")
    
    finally:
        # Clean up temporary files
        if os.path.exists(local_source_path):
            os.remove(local_source_path)
        if os.path.exists(local_transformed_path):
            os.remove(local_transformed_path)

if __name__ == '__main__':
    main()