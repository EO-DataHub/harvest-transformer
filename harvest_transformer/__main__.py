import logging
import os

import boto3
from eodhp_utils import run
from pulsar import Client

from .transformer_messager import TransformerMessager


def main():
    # Configure S3 client
    if os.getenv("AWS_ACCESS_KEY") and os.getenv("AWS_SECRET_ACCESS_KEY"):
        session = boto3.session.Session(
            aws_access_key_id=os.environ["AWS_ACCESS_KEY"],
            aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        )
        s3_client = session.client("s3")
    else:
        s3_client = boto3.client("s3")

    if os.getenv("TOPIC"):
        identifier = "_" + os.getenv("TOPIC")
    else:
        identifier = ""

    # Initiate Pulsar
    pulsar_url = os.environ.get("PULSAR_URL")
    client = Client(pulsar_url)

    producer = client.create_producer(
        topic=f"transformed{identifier}",
        producer_name=f"transformer{identifier}",
        chunking_enabled=True,
    )

    logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")

    destination_bucket = os.environ.get("S3_BUCKET")

    transformer_messager = TransformerMessager(
        s3_client=s3_client,
        output_bucket=destination_bucket,
        cat_output_prefix="transformed",
        producer=producer,
    )

    run(
        {f"harvested{identifier}": transformer_messager},
    )


if __name__ == "__main__":
    main()
