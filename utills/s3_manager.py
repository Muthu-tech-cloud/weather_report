import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
import os
import dotenv

class S3Manager:

    def __init__(self, aws_access_key, aws_secret_key, aws_region='us-east-1'):
        self.aws_access_key = aws_access_key
        self.aws_secret_key = aws_secret_key
        self.aws_region = aws_region

        if not self.aws_access_key or not self.aws_secret_key:
            raise ValueError("AWS credentials must be provided")
        
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=self.aws_access_key,
            aws_secret_access_key=self.aws_secret_key,
            region_name=self.aws_region
        )

    def create_bucket(self, bucket_name):
        try:
            if self.aws_region == 'us-east-1':
                response = self.s3_client.create_bucket(
                    Bucket=bucket_name
                )
            else:
                response = self.s3_client.create_bucket(
                    Bucket=bucket_name,
                    CreateBucketConfiguration={'LocationConstraint': self.aws_region}
                )
            print(f"Bucket {bucket_name} created successfully.")
            print(response)
        except NoCredentialsError:
            print("Credentials not available.")
        except PartialCredentialsError:
            print("Incomplete credentials provided.")
        except Exception as e:
            print(f"An error occurred while creating the bucket: {e}")

if __name__ == "__main__":
    dotenv.load_dotenv()
    aws_access_key = os.getenv('AWS_ACCESS_KEY')
    aws_secret_key = os.getenv('AWS_SECRET_KEY')
    aws_region = os.getenv('AWS_REGION')
    s3_manager = S3Manager(
        aws_access_key=aws_access_key, 
        aws_secret_key=aws_secret_key,
        aws_region=aws_region
    )
    s3_manager.create_bucket(
        bucket_name='weather-data-bucket-unique-123456')