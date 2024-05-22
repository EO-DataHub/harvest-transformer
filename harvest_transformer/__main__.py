import argparse
import copy
import json
import logging
import os
from typing import Union
from urllib.parse import urlparse

import boto3
from botocore.exceptions import ClientError
from pulsar import Client, Message

from eodhp_utils.pulsar.messages import get_message_data, harvest_schema

from .link_processor import LinkProcessor
from .utils import get_file_from_url
from .workflow_processor import WorkflowProcessor

# configure boto3 logging
logging.getLogger("botocore").setLevel(logging.CRITICAL)
logging.getLogger("boto3").setLevel(logging.CRITICAL)

# configure urllib logging
logging.getLogger("urllib3").setLevel(logging.CRITICAL)

if os.getenv("AWS_ACCESS_KEY") and os.getenv("AWS_SECRET_ACCESS_KEY"):
    session = boto3.session.Session(
        aws_access_key_id=os.environ["AWS_ACCESS_KEY"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    )
    s3_client = session.client("s3")

else:
    s3_client = boto3.client("s3")
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")


def upload_file_s3(body: str, bucket: str, key: str):
    """Upload data to an S3 bucket."""
    try:
        s3_client.put_object(Body=body, Bucket=bucket, Key=key)
    except ClientError as e:
        logging.error(f"File upload failed: {e}")


def delete_file_s3(bucket: str, key: str):
    """Delete data in an S3 bucket."""
    try:
        s3_client.delete_object(Bucket=bucket, Key=key)
        logging.info(f"Deleted file {key} from bucket {bucket}.")
    except ClientError as e:
        logging.error(f"File deletion failed: {e}")


def transform_key(file_name: str, source: str, target: str) -> str:
    """Creates a key in a transformed subdirectory from a given file name"""
    transformed_key = file_name.replace("git-harvester", "transformed", 1)
    if transformed_key == file_name:
        transformed_key = "transformed" + file_name.replace(source, target)
    return transformed_key


def get_file_s3(bucket: str, key: str) -> str:
    """Retrieve data from an S3 bucket."""
    try:
        file_obj = s3_client.get_object(Bucket=bucket, Key=key)
        return file_obj["Body"].read().decode("utf-8")
    except ClientError as e:
        logging.error(f"File retrieval failed: {e}")
        return None


def is_valid_url(url: str) -> bool:
    """Checks if a given URL is valid"""
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except ValueError:
        return False


def get_file_contents_as_json(bucket_name: str, file_location: str, updated_key: str) -> dict:
    """Returns JSON object of contents located at file_location"""
    if is_valid_url(file_location):
        file_contents = get_file_from_url(file_location)
    else:
        file_contents = get_file_s3(bucket_name, file_location)
    try:
        return json.loads(file_contents)
    except ValueError:
        # Invalid JSON. File returned as is.
        logging.info(f"File {file_location} is not valid JSON.")
        return file_contents


def update_file(
    file_name: str,
    source: str,
    target_location: str,
    file_body: Union[dict, str],
    output_root: str,
    processors: list,
) -> str:
    """
    Updates content within a given file name. File name may either be a URL or S3 key.
    Uploads updated file contents to updated_key within the given bucket.
    """

    for processor in processors:
        file_body = processor.update_file(
            file_name=file_name,
            source=source,
            target_location=target_location,
            file_body=file_body,
            output_root=output_root,
        )

    # Convert json to string for file upload
    if isinstance(file_body, dict):
        file_body = json.dumps(file_body)
    return file_body


def add_or_update_keys(key: str, source: str, target: str, bucket_name: str, processors: list):
    # Generate transformed key
    updated_key = transform_key(key, source, target)

    # Compose target_location
    target_location = args.output_root + target

    # Apply transformers
    file_body = get_file_contents_as_json(bucket_name, key, updated_key)
    file_body = update_file(key, source, target_location, file_body, args.output_root, processors)

    # Upload file to S3
    upload_file_s3(file_body, bucket_name, updated_key)
    logging.info(f"Links successfully rewritten for file {key}")

    return updated_key


def process_pulsar_message(msg: Message):
    """
    Update files from given message where required,
    and send a Pulsar message with updated files
    """
    data_dict = get_message_data(msg, harvest_schema)

    bucket_name = data_dict.get("bucket_name")
    source = data_dict.get("source")
    target = data_dict.get("target")

    output_data = copy.deepcopy(data_dict)
    output_data["added_keys"] = []
    output_data["updated_keys"] = []
    output_data["deleted_keys"] = []

    processors = [WorkflowProcessor(), LinkProcessor()]

    for key in data_dict.get("added_keys"):
        updated_key = add_or_update_keys(key, source, target, bucket_name, processors)
        output_data["added_keys"].append(updated_key)

    for key in data_dict.get("updated_keys"):
        updated_key = add_or_update_keys(key, source, target, bucket_name, processors)
        output_data["updated_keys"].append(updated_key)

    for key in data_dict.get("deleted_keys"):
        # Generate transformed key
        updated_key = transform_key(key, source, target)
        # Remove file from S3
        delete_file_s3(bucket_name, updated_key)
        output_data["deleted_keys"].append(updated_key)

    # Send message to Pulsar
    producer.send((json.dumps(output_data)).encode("utf-8"))
    logging.info(f"Sent transformed message {output_data}")


def main():
    """
    Poll for new Pulsar messages and trigger transform process
    """
    while True:
        msg = consumer.receive()
        try:
            logging.info(f"Parsing harvested message {msg.data()}")
            process_pulsar_message(msg)

            # Acknowledge successful processing of the message
            consumer.acknowledge(msg)
        except Exception as e:
            # Message failed to be processed. Acknowledge to remove it.
            logging.error(f"Error occurred during transform: {e}")
            consumer.acknowledge(msg)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("output_root", help="Root URL for EODHP", type=str)
    args = parser.parse_args()

    if os.getenv("TOPIC"):
        identifier = "_" + os.getenv("TOPIC")
    else:
        identifier = ""

    # Initiate Pulsar
    pulsar_url = os.environ.get("PULSAR_URL")
    client = Client(pulsar_url)
    consumer = client.subscribe(
        topic=f"harvested{identifier}", subscription_name=f"transformer-subscription{identifier}"
    )
    producer = client.create_producer(
        topic=f"transformed{identifier}", producer_name=f"transformer{identifier}"
    )
    main()
