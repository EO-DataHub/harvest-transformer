import argparse
import copy
import json
import logging
import os
import urllib.request
from urllib.parse import urljoin, urlparse
from urllib.request import urlopen

import boto3
import jsonschema
import jsonschema.exceptions
from botocore.exceptions import ClientError
from pulsar import Client, Message

from harvest_transformer.pulsar_message import harvest_schema

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
    topic="harvested", subscription_name=f"transformer-subscription{identifier}"
)
producer = client.create_producer(topic="transformed", producer_name=f"transformer{identifier}")


if os.getenv("AWS_ACCESS_KEY") and os.getenv("AWS_SECRET_ACCESS_KEY"):
    session = boto3.session.Session(
        aws_access_key_id=os.environ["AWS_ACCESS_KEY"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    )
    s3_client = session.client("s3")

else:
    s3_client = boto3.client("s3")
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


def transform_key(file_name: str, source: str, target: str) -> str:
    """Creates a key in a transformed subdirectory from a given file name"""
    transformed_key = file_name.replace("git-harvester", "transformed", 1)
    if transformed_key == file_name:
        transformed_key = "transformed" + file_name.replace(source, target)
    return transformed_key


def delete_sections(json_data: dict) -> dict:
    """Remove all unnecessary data from a file."""
    json_data.pop("conformsTo", None)
    return json_data


def find_all_links(node):
    """Recursively find all nested links in a given item"""
    if isinstance(node, list):
        for i in node:
            for x in find_all_links(i):
                yield x
    elif isinstance(node, dict):
        if "links" in node:
            for link in node["links"]:
                yield link
        for j in node.values():
            for x in find_all_links(j):
                yield x


def rewrite_links(json_data: dict, source: str, target_location: str, output_self: str) -> dict:
    """Rewrite links so that they are suitable for an EODHP catalogue"""
    for link in find_all_links(json_data):
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


def get_file_from_url(url: str, retries: int = 0) -> str:
    """Returns contents of data available at given URL"""
    if retries == 3:
        # Max number of retries
        return None
    try:
        with urlopen(url, timeout=5) as response:
            body = response.read()
    except urllib.error.URLError:
        logging.error(f"Unable to access {url}, retrying...")
        return get_file_from_url(url, retries + 1)
    return body.decode("utf-8")


def get_file_contents_as_json(bucket_name: str, file_location: str, updated_key: str) -> dict:
    """Returns JSON object of contents located at file_location"""
    if is_valid_url(file_location):
        file_contents = get_file_from_url(file_location)
    else:
        file_contents = get_file_s3(bucket_name, file_location)
    try:
        return json.loads(file_contents)
    except ValueError:
        # Invalid JSON. Upload without changes
        logging.info(f"File {file_location} is not valid JSON.")
        upload_file_s3(file_contents, bucket_name, updated_key)
        return


def update_file(bucket_name: str, file_name: str, updated_key: str, source: str, target: str):
    """
    Updates content within a given file name. File name may either be a URL or S3 key.
    Uploads updated file contents to updated_key within the given bucket.
    """
    target_location = args.output_root + target

    file_json = get_file_contents_as_json(bucket_name, file_name, updated_key)

    # Delete unnecessary sections
    file_json = delete_sections(file_json)

    try:
        self_link = [
            link.get("href") for link in file_json.get("links") if link.get("rel") == "self"
        ][0]
    except (TypeError, IndexError):
        logging.error(f"File {file_name} does not contain a self link. Unable to rewrite links.")
        file_body = json.dumps(file_json)
        upload_file_s3(file_body, bucket_name, updated_key)
        return

    output_self = self_link.replace(source, target_location)
    if not is_valid_url(output_self):
        logging.error(
            f"File {file_name} does not produce a valid self link with given "
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
    logging.info(f"Links successfully rewritten for file {file_name}")


def process_pulsar_message(msg: Message):
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

    for key in data_dict.get("added_keys"):
        updated_key = transform_key(key, source, target)
        update_file(bucket_name, key, updated_key, source, target)
        output_data["added_keys"].append(updated_key)
    for key in data_dict.get("updated_keys"):
        updated_key = transform_key(key, source, target)
        update_file(bucket_name, key, updated_key, source, target)
        output_data["updated_keys"].append(updated_key)

    for key in data_dict.get("deleted_keys"):
        updated_key = transform_key(key, source, target)
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
    main()
