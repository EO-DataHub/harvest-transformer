import logging
import os
import uuid
from pathlib import Path

import botocore
import botocore.client
import click
from eodhp_utils.runner import (
    get_boto3_session,
    get_pulsar_client,
    log_component_version,
    run,
    setup_logging,
)

from harvest_transformer.link_processor import LinkProcessor
from harvest_transformer.qa_processor import QAProcessor
from harvest_transformer.render_processor import RenderProcessor
from harvest_transformer.utils import load_json_file
from harvest_transformer.workflow_processor import WorkflowProcessor

from .transformer_messager import TransformerMessager


@click.command
@click.option("-v", "--verbose", count=True)
@click.option("-t", "--threads", type=int, default=1)
def main(verbose: int, threads: int) -> None:
    setup_logging(verbosity=verbose)
    log_component_version("harvest-transformer")

    # Configure S3 client.
    s3_client = get_boto3_session().client(
        "s3", config=botocore.client.Config(max_pool_connections=max(threads * 2, 10))
    )

    if os.getenv("TOPIC"):
        identifier = "_" + os.getenv("TOPIC", "")
    else:
        identifier = ""

    producer_id = os.getenv("PRODUCER_UNIQUE_SUFFIX", str(uuid.uuid4()))

    # Initiate Pulsar
    pulsar_client = get_pulsar_client(message_listener_threads=threads)

    producer = pulsar_client.create_producer(
        topic=f"transformed{identifier}",
        producer_name=f"transformer{identifier}-{producer_id}",
        chunking_enabled=True,
    )

    destination_bucket = os.environ.get("S3_BUCKET")
    qa_mapping_path = os.getenv(
        "QA_COLLECTION_MAP_FILE",
        str(Path(__file__).resolve().parent.parent / "qa-collection-map.json"),
    )
    qa_asset_root = os.getenv("QA_ASSET_ROOT")

    try:
        qa_collection_map = load_json_file(qa_mapping_path)
        if not isinstance(qa_collection_map, dict):
            raise TypeError("QA collection map must be a JSON object")
    except (OSError, ValueError, TypeError) as exc:
        qa_collection_map = {}
        logging.warning(f"Unable to load QA collection map from {qa_mapping_path}: {exc}")

    transformer_messager = TransformerMessager(
        processors=[
            WorkflowProcessor(),
            LinkProcessor(s3_client=s3_client),
            QAProcessor(collection_map=qa_collection_map, asset_root=qa_asset_root),
            RenderProcessor(),
        ],
        s3_client=s3_client,
        output_bucket=destination_bucket,
        cat_output_prefix="transformed/",
        producer=producer,
    )

    run(
        {f"harvested{identifier}": transformer_messager},
        subscription_name=f"transformer{identifier}",
        threads=threads,
    )


if __name__ == "__main__":
    main()
