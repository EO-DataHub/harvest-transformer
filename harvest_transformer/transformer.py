import json
import logging
import os
from typing import Union
from urllib.parse import urljoin, urlparse

import boto3

from .link_processor import LinkProcessor
from .render_processor import RenderProcessor
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
    new_id = target.rstrip("/").split("/")[-1]

    if new_id == "":
        return None
    return new_id


def transform_key(file_name: str, source: str, target: str) -> str:
    """Creates a key in a transformed subdirectory from a given file name"""
    transformed_key = file_name.replace("git-harvester/", "", 1)
    if transformed_key == file_name:
        transformed_key = file_name.replace(source, target, 1)
    transformed_key = reformat_key(transformed_key)
    return transformed_key


def is_valid_url(url: str) -> bool:
    """Checks if a given URL is valid"""
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except ValueError:
        return False


def update_file(
    file_name: str,
    source: str,
    target_location: str,
    entry_body: Union[dict, str],
    output_root: str,
    processors: list,
) -> str:
    """
    Updates content within a given file name. File name may either be a URL or S3 key.
    Uploads updated file contents to updated_key within the given bucket.
    """

    for processor in processors:
        entry_body = processor.update_file(
            file_name=file_name,
            source=source,
            target_location=target_location,
            entry_body=entry_body,
            output_root=output_root,
        )

    # Convert json to string for file upload
    if isinstance(entry_body, dict):
        entry_body = json.dumps(entry_body)

    return entry_body


def is_this_root_catalog(entry_body: dict) -> bool:
    """Check if the current STAC file is also the root catalog definition"""
    cat_links = entry_body.get("links")
    if not cat_links:
        return False
    root_href = None
    self_href = None
    for link in cat_links:
        if link.get("rel") == "root":
            root_href = link.get("href")
        elif link.get("rel") == "self":
            self_href = link.get("href")
        if root_href and self_href:
            break
    if not root_href or not self_href:
        return False
    return root_href == self_href


def update_catalog_id(entry_body: dict, target: str) -> dict:
    """Update catalog ID in entry_body to match target"""
    if entry_body.get("type") != "Catalog" or not is_this_root_catalog(entry_body):
        return entry_body
    new_catalog_id = get_new_catalog_id_from_target(target)

    # Update catalog_id if new one is provided in target
    if new_catalog_id:
        entry_body["id"] = new_catalog_id
    return entry_body


def transform(
    file_name: str,
    entry_body: Union[dict, str],
    source: str,
    target: str,
    output_root: str,
    workspace: str,
):
    """Load file from given key as json and update by applying list of processors"""

    # Compose target_location
    target_location = urljoin(output_root, target)

    # Update catalog ID if necessary
    if isinstance(entry_body, dict):
        entry_body = update_catalog_id(entry_body, target)

    # Define list of processors
    processors = [WorkflowProcessor(), LinkProcessor(workspace), RenderProcessor()]
    entry_body = update_file(
        file_name, source, target_location, entry_body, output_root, processors
    )

    return entry_body
