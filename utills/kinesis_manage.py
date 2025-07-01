import boto3
from dotenv import load_dotenv
import os
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

class KinesisManager:
    def __init__(self, aws_access_key, aws_secret_key, aws_region='us-east-1'):
        self.aws_access_key = aws_access_key
        self.aws_secret_key = aws_secret_key
        self.aws_region = aws_region

        if not self.aws_access_key or not self.aws_secret_key:
            raise ValueError("AWS credentials must be provided")
        
        self.kinesis_client = boto3.client(
            'kinesis',
            aws_access_key_id=self.aws_access_key,
            aws_secret_access_key=self.aws_secret_key,
            region_name=self.aws_region
        )

    def create_stream(self, stream_name, shard_count=1):
        try:
            response = self.kinesis_client.create_stream(
                StreamName=stream_name,
                ShardCount=shard_count
            )
            print(f"Stream {stream_name} created successfully. with the shard count of {shard_count}.")
            print(response)
            
            waiter = self.kinesis_client.get_waiter('stream_exists')
            waiter.wait(StreamName=stream_name)
            print(f"Stream {stream_name} is now active.")
        except NoCredentialsError:
            print("Credentials not available.")
        except PartialCredentialsError:
            print("Incomplete credentials provided.")
        except Exception as e:
            print(f"An error occurred while creating the stream: {e}")


if __name__ == "__main__":

    load_dotenv()
    aws_access_key = os.getenv('AWS_ACCESS_KEY')
    aws_secret_key = os.getenv('AWS_SECRET_KEY')    
    aws_region = os.getenv('AWS_REGION') 
    kinesis_manager = KinesisManager(
        aws_access_key=aws_access_key, 
        aws_secret_key=aws_secret_key,
        aws_region=aws_region
    )
    kinesis_manager.create_stream(
        stream_name='weather_data_stream', shard_count=1
    )