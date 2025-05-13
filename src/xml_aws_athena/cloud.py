import boto3
from botocore.exceptions import ClientError
import logging
from io import BytesIO


class Storage:
    def __init__(self, **kwargs) -> None:
        self.kwargs = kwargs
        self.s3_client = boto3.client("s3", **kwargs)

    def create_bucket(self, bucket_name: str, region: str = None) -> bool:
        """
        Create an S3 bucket in a specified region.
        If a region is not specified, the bucket will be created in the default region.
        """
        try:
            if region := self.kwargs.get("region_name", None):
                location = {"LocationConstraint": region}
                self.s3_client.create_bucket(
                    Bucket=bucket_name, CreateBucketConfiguration=location
                )
            else:
                self.s3_client.create_bucket(Bucket=bucket_name)
            return True
        except ClientError as e:
            logging.error(f"Error creating bucket: {e}")
            return False

    def delete_bucket_objects(self, bucket_name: str) -> bool:
        """
        Delete all objects in an S3 bucket.
        This function does not delete the bucket itself.
        It only deletes the objects inside the bucket.
        """
        try:
            kwargs = {"Bucket": bucket_name}

            while True:
                objects_to_delete = self.s3_client.list_objects_v2(**kwargs)

                if "Contents" in objects_to_delete:
                    match objects_to_delete:
                        case {"Contents": obj_delete}:
                            for obj in obj_delete:
                                self.s3_client.delete_object(
                                    Bucket=bucket_name, Key=obj["Key"]
                                )

                    token = objects_to_delete.get("NextContinuationToken", None)
                    if token is None:
                        break

                    kwargs["ContinuationToken"] = token

                else:
                    break

            # After deleting all objects, delete the bucket itself
            self.s3_client.delete_bucket(Bucket=bucket_name)
            return True
        except ClientError as e:
            logging.error(f"Error deleting bucket objects: {e}")
            return False

    def put_object_file(
        self, body: str | BytesIO, bucket_name: str, object_key: str
    ) -> bool:
        """
        Upload a file to an S3 bucket.
        """
        try:
            self.s3_client.put_object(Bucket=bucket_name, Key=object_key, Body=body)
            return True
        except ClientError as e:
            logging.error(f"Error uploading file to S3: {e}")
            return False
