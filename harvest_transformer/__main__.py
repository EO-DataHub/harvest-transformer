import logging
import os

from eodhp_utils.runner import get_boto3_session, get_pulsar_client, run

from .transformer_messager import TransformerMessager


def main():
    # Configure S3 client
    s3_client = get_boto3_session().client("s3")

    if os.getenv("TOPIC"):
        identifier = "_" + os.getenv("TOPIC")
    else:
        identifier = ""

    # Initiate Pulsar
    pulsar_client = get_pulsar_client()

    producer = pulsar_client.create_producer(
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
