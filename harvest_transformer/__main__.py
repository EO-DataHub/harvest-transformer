import argparse
import copy
import json
import logging
import os
from urllib.parse import urljoin, urlparse

import boto3
import jsonschema
import jsonschema.exceptions
from botocore.exceptions import ClientError
from pulsar import Client

from harvest_transformer.pulsar_message import harvest_schema

parser = argparse.ArgumentParser()
parser.add_argument("output_root", help="Root URL for EODHP", type=str)
args = parser.parse_args()

# Initiate Pulsar
pulsar_url = os.environ.get("PULSAR_URL")
client = Client(pulsar_url)
consumer = client.subscribe(topic="harvested", subscription_name="transformer-subscription")
producer = client.create_producer(topic="transformed", producer_name="transformer")

s3_client = boto3.client(
    "s3",
    aws_access_key_id=os.environ.get("AWS_ACCESS_KEY"),
    aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
)
logging.basicConfig(level=logging.DEBUG, format="%(message)s")


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


def transform_key(key: str) -> str:
    """Moves a given key to a transformed subdirectory"""
    transformed_key = key.replace("git-harvester", "transformed", 1)
    if transformed_key == key:
        transformed_key = "transformed/" + key
    return transformed_key


def delete_sections(json_data: dict) -> dict:
    """Remove all unnecessary data from a file."""
    json_data.pop("conformsTo", None)
    return json_data


def rewrite_links(json_data: dict, source: str, target_location: str, output_self: str) -> dict:
    """Rewrite links so that they are suitable for an EODHP catalogue"""
    for link in json_data.get("links"):
        if not link.get("href").startswith(args.output_root):
            if link.get("href").startswith(source):
                # Link is an absolute link. Replace the source.
                link["href"] = link["href"].replace(source, target_location)
            elif link.get("rel") == "parent":
                link["href"] = output_self.rsplit("/", 2)[0]
            elif link.get("href").startswith("http"):
                # Link is an absolute link with a different root than expected.
                # Assume valid link outside of EODH
                pass
            else:
                # Link is a relative link. Convert to absolute link.
                link["href"] = urljoin(output_self.rsplit("/", 1)[0], link.get("href"))
    return json_data


def add_missing_links(json_data: dict, new_root: str, new_self: str) -> dict:
    """As per STAC best practices, ensure root and self links exist."""
    links = json_data.get("links", [])

    add_link_if_missing(links, "root", new_root)
    add_link_if_missing(links, "self", new_self)

    return json_data


def add_link_if_missing(links: dict, rel: str, href: str):
    """Ensures a link consisting of given rel exists in links."""
    link_exists = False
    for link in links:
        if link.get("rel") == rel:
            link_exists = True
    if not link_exists:
        links.append({"rel": rel, "href": href})


def update_file(bucket_name: str, key: str, updated_key: str, source: str, target: str):
    """
    Updates content within a file at given bucket and key.
    Uploads updated file contents to updated_key within the same bucket.
    """
    file_contents = get_file_s3(bucket_name, key)
    target_location = args.output_root + target
    try:
        file_json = json.loads(file_contents)
    except ValueError:
        # Invalid JSON. Upload without changes
        logging.info(f"File {key} is not valid JSON.")
        file_body = json.dumps(file_json)
        upload_file_s3(file_body, bucket_name, updated_key)
        return

    # Delete unnecessary sections
    file_json = delete_sections(file_json)

    try:
        self_link = [
            link.get("href") for link in file_json.get("links") if link.get("rel") == "self"
        ][0]
    except (TypeError, IndexError):
        logging.error(f"File {key} does not contain a self link. Unable to rewrite links.")
        file_body = json.dumps(file_json)
        upload_file_s3(file_body, bucket_name, updated_key)
        return

    output_self = self_link.replace(source, target_location)
    if not is_valid_url(output_self):
        logging.error(
            f"File {key} does not produce a valid self link with given "
            f"self link {self_link}, source {source}, and target {target_location}. "
            f"Unable to rewrite links."
        )
        file_body = json.dumps(file_json)
        upload_file_s3(file_body, bucket_name, updated_key)
        return

    # Update links to STAC best practices
    file_json = add_missing_links(file_json, args.output_root, output_self)

    # Update links to refer to EODH
    file_json = rewrite_links(file_json, source, target_location, output_self)

    # Upload file to S3
    file_body = json.dumps(file_json)
    upload_file_s3(file_body, bucket_name, updated_key)
    logging.info(f"Links successfully rewritten for file {key}")


def process_pulsar_message(msg):
    """
    Update files from given message where required,
    and send a Pulsar message with updated files
    """
    data = msg.data().decode("utf-8")
    data_dict = json.loads(data.replace("'", '"'))
    try:
        jsonschema.validate(data_dict, harvest_schema)
    except jsonschema.exceptions.ValidationError as e:
        logging.error(f"Validation failed: {e}")
        raise

    bucket_name = data_dict.get("bucket_name")
    source = data_dict.get("source")
    target = data_dict.get("target")

    output_data = copy.deepcopy(data_dict)
    output_data["added_keys"] = []
    output_data["updated_keys"] = []
    output_data["deleted_keys"] = []

    for key in data_dict["added_keys"]:
        updated_key = transform_key(key)
        update_file(bucket_name, key, updated_key, source, target)
        output_data["added_keys"].append(updated_key)
    for key in data_dict["updated_keys"]:
        updated_key = transform_key(key)
        update_file(bucket_name, key, updated_key, source, target)
        output_data["updated_keys"].append(updated_key)
    for key in data_dict["deleted_keys"]:
        updated_key = transform_key(key)
        delete_file_s3(bucket_name, updated_key, source, target)
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
    main()
