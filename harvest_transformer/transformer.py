import json
import logging
import os
from typing import Union
from urllib.parse import urljoin, urlparse

import botocore
import botocore.exceptions
import jsonpatch


def reformat_key(key: str) -> str:
    """Reformat key to remove trailing slashes and add file extension"""

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
    transformed_key = (
        file_name.replace("git-harvester/", "", 1)
        .replace("file-harvester/", "", 1)
        .replace("stac-harvester/", "", 1)
    )
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
    workspace: str,
) -> str:
    """
    Updates content within a given file name. File name is an S3 key.
    Uploads updated file contents to updated_key within the given bucket.
    """

    for processor in processors:
        entry_body = processor.update_file(
            file_name=file_name,
            source=source,
            target_location=target_location,
            entry_body=entry_body,
            output_root=output_root,
            workspace=workspace,
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


def get_patch(s3_client, cat_path: str) -> Union[dict, None]:
    """
    Check if a patch exists for a given collection inside the patches/ folder in S3.
    """
    patch_bucket = os.getenv("PATCH_BUCKET") or os.getenv("S3_BUCKET")
    patch_prefix = os.getenv("PATCH_PREFIX", "patches")
    patch_key = f"{patch_prefix}/{cat_path}"

    try:
        response = s3_client.get_object(Bucket=patch_bucket, Key=patch_key)
        patch_data = json.loads(response["Body"].read().decode("utf-8"))
        logging.info(f"Patch found for collection: {patch_key}")
        return patch_data
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            logging.info(f"No patch found for collection: {patch_key}")
            return None
        else:
            logging.exception(f"Unexpected error retrieving patch for {patch_key}: {e}")
            return None
    except Exception as e:
        logging.exception(f"Error retrieving patch for {patch_key}: {e}")
        return None


def apply_patch(original: dict, patch: list) -> dict:
    """Apply a JSON patch ensuring output remains a dictionary."""
    try:
        patch_obj = jsonpatch.JsonPatch(patch)
        patched = patch_obj.apply(original)
        return patched
    except jsonpatch.JsonPatchException as e:
        logging.error(f"Error applying patch: {e}")
        return original


def transform(
    processors,
    file_name: str,
    entry_body: Union[dict, str],
    source: str,
    target: str,
    output_root: str,
    workspace: str,
    s3_client,
):
    """Load file from given key as JSON and update by applying list of processors"""

    patch_data = None

    # Check for a patch and apply it
    if isinstance(entry_body, dict) and entry_body.get("type") == "Collection":
        patch_data = get_patch(s3_client, file_name)

        # If patch data exists, apply it to entry_body
        if patch_data:
            entry_body = apply_patch(entry_body, patch_data)

    # Compose target_location
    target_location = urljoin(output_root, target)

    # Update catalog ID if necessary
    # if isinstance(entry_body, dict):
    #     entry_body = update_catalog_id(entry_body, target)
    entry_body = update_file(
        file_name,
        source,
        target_location,
        entry_body,
        output_root,
        processors,
        workspace,
    )

    return entry_body
