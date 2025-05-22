import os
import uuid

import click
from eodhp_utils.runner import (
    get_boto3_session,
    get_pulsar_client,
    log_component_version,
    run,
    setup_logging,
)

from .transformer_messager import TransformerMessager


@click.command
@click.option("-v", "--verbose", count=True)
@click.option("-t", "--threads", type=int, default=1)
def main(verbose: int, threads: int):
    setup_logging(verbosity=verbose)
    log_component_version("harvest_transformer")

    # Configure S3 client
    s3_client = get_boto3_session().client("s3")

    if os.getenv("TOPIC"):
        identifier = "_" + os.getenv("TOPIC")
    else:
        identifier = ""

    producer_id = os.getenv("PRODUCER_UNIQUE_SUFFIX", str(uuid.uuid4()))

    # Initiate Pulsar
    pulsar_client = get_pulsar_client()

    producer = pulsar_client.create_producer(
        topic=f"transformed{identifier}",
        producer_name=f"transformer{identifier}-{producer_id}-{str(uuid.uuid4())}",
        chunking_enabled=True,
    )

    destination_bucket = os.environ.get("S3_BUCKET")

    transformer_messager = TransformerMessager(
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
