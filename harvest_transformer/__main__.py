import argparse
import copy
import json
import logging
import os
from typing import Union
from urllib.parse import urlparse

import boto3
from botocore.exceptions import ClientError
from eodhp_utils.pulsar.messages import generate_harvest_schema, get_message_data
from pulsar import Client, ConsumerDeadLetterPolicy, ConsumerType, Message

from .link_processor import LinkProcessor
from .render_processor import RenderProcessor
from .utils import URLAccessError, get_file_from_url
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
        raise


def delete_file_s3(bucket: str, key: str):
    """Delete data in an S3 bucket."""
    try:
        s3_client.delete_object(Bucket=bucket, Key=key)
        logging.info(f"Deleted file {key} from bucket {bucket}.")
    except ClientError as e:
        logging.error(f"File deletion failed: {e}")
        raise


def reformat_key(key: str) -> str:
    """Reformat key to conform to nested catalog/collections standard for EODHP"""
    key = key.replace("/items", "")
    key = key.replace("/collections", "")

    if key.endswith("/"):
        key = key[:-1]

    if not key.endswith(".json"):
        key = key + ".json"

    return key


def get_new_catalog_id_from_target(target: str) -> str:
    """Extract catalog ID from target"""
    # Currently take catalog_id directly under top-level catalog,
    # as current harvested catalogs do not support nesting
    new_id = target.split("/")[-1]

    if new_id == "":
        return None
    return new_id


def transform_key(file_name: str, source: str, target: str) -> str:
    """Creates a key in a transformed subdirectory from a given file name"""
    transformed_key = file_name.replace("git-harvester", "transformed", 1)
    if transformed_key == file_name:
        transformed_key = "transformed/" + file_name.replace(source, target)
    transformed_key = reformat_key(transformed_key)
    return transformed_key


def get_file_s3(bucket: str, key: str) -> str:
    """Retrieve data from an S3 bucket."""
    try:
        file_obj = s3_client.get_object(Bucket=bucket, Key=key)
        return file_obj["Body"].read().decode("utf-8")
    except ClientError as e:
        logging.error(f"File retrieval failed: {e}")
        raise


def is_valid_url(url: str) -> bool:
    """Checks if a given URL is valid"""
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except ValueError:
        return False


def get_file_contents_as_json(file_location: str, bucket_name: str = None) -> dict:
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
    except URLAccessError:
        # Invalid URL. Raise error for notification
        logging.error(f"File {file_location} is invalid.")
        raise


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


def is_this_root_catalog(file_body: dict, key: str) -> bool:
    """Check if the current STAC file is also the root catalog definition"""
    cat_links = file_body.get("links")
    if not cat_links:
        return False
    for link in cat_links:
        if link.get("rel") == "root":
            try:
                trimmed_link = link.get("href").rstrip("/")
            except AttributeError as e:
                logging.warning(f"Error occurred during root link extraction for {key}: {e}")
                return False
            trimmed_key = key.rstrip("/")
            if trimmed_link == trimmed_key:
                return True
    return False


def update_catalog_id(file_body: dict, target: str, key: str) -> dict:
    """Update catalog ID in file_body to match target"""
    if file_body.get("type") != "Catalog" or not is_this_root_catalog(file_body, key):
        return file_body
    new_catalog_id = get_new_catalog_id_from_target(target)

    # Update catalog_id if new one is provided in target
    if new_catalog_id:
        file_body["id"] = new_catalog_id
    return file_body


def add_or_update_keys(
    key: str,
    source: str,
    target: str,
    output_root: str,
    bucket_name: str,
    processors: list,
):
    """Load file from given key as json and update by applying list of processors"""

    # Compose target_location
    target_location = output_root + target

    # Apply transformers
    file_body = get_file_contents_as_json(key, bucket_name)

    # Update catalog ID if necessary
    file_body = update_catalog_id(file_body, target, key)

    file_body = update_file(key, source, target_location, file_body, output_root, processors)

    return file_body


def process_pulsar_message(msg: Message, output_root: str):
    """
    Update files from given message where required,
    and send a Pulsar message with updated files
    """
    harvest_schema = generate_harvest_schema()
    data_dict = get_message_data(msg, harvest_schema)

    bucket_name = data_dict.get("bucket_name")
    source = data_dict.get("source")
    target = data_dict.get("target")

    output_data = copy.deepcopy(data_dict)
    output_data["added_keys"] = []
    output_data["updated_keys"] = []
    output_data["deleted_keys"] = []
    output_data["failed_files"] = {
        "temp_failed_keys": {
            "updated_keys": [],
            "added_keys": [],
            "deleted_keys": [],
        },
        "perm_failed_keys": {
            "updated_keys": [],
            "added_keys": [],
            "deleted_keys": [],
        },
    }
    error_data = copy.deepcopy(output_data)
    processors = [WorkflowProcessor(), LinkProcessor(), RenderProcessor()]

    for key in data_dict.get("added_keys"):
        try:
            updated_key = transform_key(key, source, target)
            file_body = add_or_update_keys(
                key, source, target, output_root, bucket_name, processors
            )

            # Upload file to S3
            upload_file_s3(file_body, bucket_name, updated_key)
            logging.info(f"Links successfully rewritten for file {key}")
            output_data["added_keys"].append(updated_key)
        except URLAccessError as e:
            logging.error(f"Unable to access key {key}: {e}")
            error_data["failed_files"]["perm_failed_keys"]["added_keys"].append(key)
            continue
        except ClientError as e:
            logging.error(f"Temporary error processing added key {key}: {e}")
            output_data["failed_files"]["temp_failed_keys"]["added_keys"].append(key)
            continue
        except Exception as e:
            logging.exception(f"Permanent error processing added key {key}: {e}")
            output_data["failed_files"]["perm_failed_keys"]["added_keys"].append(key)
            continue

    for key in data_dict.get("updated_keys"):
        try:
            updated_key = transform_key(key, source, target)
            file_body = add_or_update_keys(
                key, source, target, output_root, bucket_name, processors
            )

            # Upload file to S3
            upload_file_s3(file_body, bucket_name, updated_key)
            logging.info(f"Links successfully rewritten for file {key}")
            output_data["updated_keys"].append(updated_key)
        except URLAccessError as e:
            logging.error(f"Unable to access key {key}: {e}")
            error_data["failed_files"]["perm_failed_keys"]["updated_keys"].append(key)
            continue
        except ClientError as e:
            logging.error(f"Temporary error processing updated key {key}: {e}")
            output_data["failed_files"]["temp_failed_keys"]["updated_keys"].append(key)
            continue
        except Exception as e:
            logging.exception(f"Permanent error processing updated key {key}: {e}")
            output_data["failed_files"]["perm_failed_keys"]["updated_keys"].append(key)
            continue

    for key in data_dict.get("deleted_keys"):
        try:
            # Generate transformed key
            updated_key = transform_key(key, source, target)
            # Remove file from S3
            delete_file_s3(bucket_name, updated_key)
            output_data["deleted_keys"].append(updated_key)
        except URLAccessError as e:
            logging.error(f"Unable to access key {key}: {e}")
            error_data["failed_files"]["perm_failed_keys"]["deleted_keys"].append(key)
            continue
        except ClientError as e:
            logging.error(f"Temporary error processing deleted key {key}: {e}")
            output_data["failed_files"]["temp_failed_keys"]["deleted_keys"].append(key)
            continue
        except Exception as e:
            logging.exception(f"Permanent error processing deleted key {key}: {e}")
            output_data["failed_files"]["perm_failed_keys"]["deleted_keys"].append(key)
            continue

    return output_data, error_data


def main():
    """
    Poll for new Pulsar messages and trigger transform process
    """
    while True:
        msg = consumer.receive()
        # Send message to Pulsar

        output_data, error_data = process_pulsar_message(msg, args.output_root)

        try:
            data = json.dumps(output_data).encode("utf-8")
        except (ValueError, UnicodeEncodeError) as e:
            logging.error("Failed to encode message output: %e", e)
            consumer.negative_acknowledge(msg)
            continue
        else:
            producer.send(data)

        logging.info(f"Sent transformed message {output_data}")
        if (
            error_data["failed_files"]["perm_failed_keys"]["added_keys"]
            or error_data["failed_files"]["perm_failed_keys"]["updated_keys"]
            or error_data["failed_files"]["perm_failed_keys"]["deleted_keys"]
        ):
            logging.info(f"URL errors found: {error_data}")
        else:
            logging.info("No URL errors found")

        if (
            output_data["failed_files"]["temp_failed_keys"]["updated_keys"]
            or output_data["failed_files"]["temp_failed_keys"]["added_keys"]
            or output_data["failed_files"]["temp_failed_keys"]["deleted_keys"]
        ):
            consumer.negative_acknowledge(msg)
        else:
            consumer.acknowledge(msg)


def check_s3_access():
    """Test S3 access"""
    try:
        s3_client.list_buckets()
        logging.info("S3 access successful.")
    except ClientError as e:
        logging.exception(f"S3 access failed: {e}")
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("output_root", help="Root URL for EODHP", type=str)
    args = parser.parse_args()

    if os.getenv("TOPIC"):
        identifier = "_" + os.getenv("TOPIC")
    else:
        identifier = ""

    # check access to S3 bucket, if not present exit
    check_s3_access()
    # Initiate Pulsar
    pulsar_url = os.environ.get("PULSAR_URL")
    client = Client(pulsar_url)
    max_redelivery_count = 3
    delay_ms = 30000
    consumer = client.subscribe(
        topic=f"harvested{identifier}",
        subscription_name=f"transformer{identifier}",
        consumer_type=ConsumerType.Shared,
        dead_letter_policy=ConsumerDeadLetterPolicy(
            max_redeliver_count=max_redelivery_count, dead_letter_topic=f"dead-letter{identifier}"
        ),
        negative_ack_redelivery_delay_ms=delay_ms,
    )

    producer = client.create_producer(
        topic=f"transformed{identifier}", producer_name=f"transformer{identifier}"
    )

    main()
