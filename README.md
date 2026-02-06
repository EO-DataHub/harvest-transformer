# Harvest Transformer

The Harvest Transformer is a service for transforming harvested SpatioTemporal Asset Catalog (STAC) metadata to make it suitable for the EODHP (Earth Observation Data Hub Platform) catalogue.

It is designed to operate as part of a data pipeline, reading messages from Apache Pulsar to discover new or updated STAC files, processing and transforming their contents, and storing the results in S3. After transformation, it sends messages back to Pulsar to notify downstream services of the new or updated catalogue entries.

The service rewrites internal links, manages licenses, and prepares data for seamless integration into the EODHP system.

## Features

- **STAC Link Rewriting:** Ensures all STAC links (`self`, `parent`, `root`, `collection`, etc.) are rewritten to point to the new catalogue location.
- **License Handling:** Maps SPDX license codes to filenames and ensures license links are present and correct.
- **Workflow Support:** Automatically completes STAC Collection definitions for workflow collections, scraping CWL scripts for metadata where available.
- **Patch Support:** Supports patching collections using JSON patches stored in S3.
- **Extensible Processing:** Modular processor classes for link, workflow, and render transformations.
- **Testing:** Comprehensive test suite using `pytest`.

## Getting Started

### Prerequisites

- Python 3.13
- [uv](https://docs.astral.sh/uv/)

### Setup

Clone the repository and run the setup using the Makefile:

```sh
git clone https://github.com/EO-DataHub/harvest-transformer.git
cd harvest-transformer
make setup
```

This will:
- Install dependencies via `uv sync`
- Install pre-commit hooks

You can safely run `make setup` repeatedly; it will only update things if needed.

## Configuration

The Harvest Transformer is configured through environment variables, command-line options and incoming Pulsar messages.

### Command-line options

- `-v`, `--verbose`: Increase logging verbosity (can be repeated).
- `-t`, `--threads`: Number of threads to use (default: 1).

### Environment Variables

- `PULSAR_URL`: The connection URL for the Apache Pulsar broker. Used to receive and send messages about catalogue updates.
- `AWS_ACCESS_KEY`: Your AWS access key ID, required for accessing S3 buckets.
- `AWS_SECRET_ACCESS_KEY`: Your AWS secret access key, required for accessing S3 buckets.
- `S3_BUCKET`: The name of the S3 bucket where transformed files will be stored.
- `OUTPUT_ROOT`: The base URL that will be used for rewriting STAC links in the transformed metadata.
- `PATCH_BUCKET` (optional): The S3 bucket containing JSON patches to apply to collections. If not set, defaults to `S3_BUCKET`.
- `PATCH_PREFIX` (optional): The prefix (folder path) within the patch bucket where patches are stored. Defaults to `patches`.
- `S3_SPDX_BUCKET` (optional): The S3 bucket containing SPDX license files for license link rewriting.
- `SPDX_LICENCE_PATH` (optional): The path within the SPDX bucket where license files are stored.
- `HOSTED_ZONE` (optional): The DNS hosted zone for output URLs, if custom domain management is required.


## Pulsar Messages

### Incoming Pulsar Messages (`harvested` topic)

The service listens for messages on the `harvested` topic. Each message is a JSON object with the following structure:

```json
{
  "id": "<unique_id>",
  "workspace": "<workspace_name>",
  "bucket_name": "<source_s3_bucket>",
  "source": "<source_url_prefix>",
  "target": "<target_url_prefix>",
  "updated_keys": ["<list/of/keys/to/process>"],
  "deleted_keys": ["<list/of/keys/to/delete>"],
  "added_keys": ["<list/of/keys/to/add>"]
}
```

- `id`: Unique identifier for the harvest event.
- `workspace`: Name of the workspace the data will be ingested into.
- `bucket_name`: S3 bucket where harvested files are stored.
- `source`: The original root URL of the harvested catalogue.
- `target`: The new root URL for the transformed catalogue.
- `updated_keys`, `deleted_keys`, `added_keys`: Lists of S3 keys for files that have been updated, deleted, or added.

### Outgoing Pulsar Messages (`transformed` topic)

After processing, the service sends a message to the `transformed` topic. The outgoing message has a similar structure, but refers to the transformed files:

```json
{
  "id": "<unique_id>",
  "workspace": "<workspace_name>",
  "bucket_name": "<destination_s3_bucket>",
  "source": "<source_url_prefix>",
  "target": "<target_url_prefix>",
  "updated_keys": ["<list/of/transformed/keys>"],
  "deleted_keys": ["<list/of/deleted/keys>"],
  "added_keys": ["<list/of/newly/added/keys>"]
}
```

- `bucket_name`: S3 bucket where transformed files are stored (may be different from the source).
- `updated_keys`, `deleted_keys`, `added_keys`: Refer to the transformed, deleted, or added files in the output catalogue.

**Note:** The service may also send "empty" catalogue change messages (with no updated, deleted, or added keys) to indicate a successful transformation with no file changes.

## Usage

The service is typically run as part of a data pipeline, but you can invoke it directly for testing or development.

Run the transformer from the command line:

```sh
uv run python -m harvest_transformer -t 10 -vv
```

## Development

- Code is in the `harvest_transformer` directory.
- Formatting and linting: [Ruff](https://docs.astral.sh/ruff/).
- Type checking: [Pyright](https://github.com/microsoft/pyright).
- Pre-commit checks are installed with `make setup`.

Useful Makefile targets:

- `make setup`: Set up or update the dev environment.
- `make test`: Run tests continuously with `pytest-watcher`.
- `make testonce`: Run tests once.
- `make check`: Run all linters, formatters, type checker, and validate pyproject.
- `make format`: Auto-fix lint issues and format code.
- `make install`: Install dependencies (frozen).
- `make update`: Update dependencies.
- `make dockerbuild`: Build a Docker image.
- `make dockerpush`: Push a Docker image.

### Project Structure

- **link_processor.py**: Handles STAC link rewriting and license link management.
- **workflow_processor.py**: Handles workflow-specific STAC transformations, including CWL scraping.
- **render_processor.py**: Enables the STAC render extension for specified collections.
- **transformer.py**: Main transformation logic, patching, and orchestration.
- **transformer_messager.py**: Handles Pulsar messaging and S3 interactions.
- **utils.py**: Utility functions.

## Testing

Run all tests with:

```sh
make testonce
```

Tests use [pytest](https://docs.pytest.org/) and [moto](https://github.com/spulec/moto) for AWS mocking.

## Troubleshooting

- **Authentication errors:** Ensure your `AWS_ACCESS_KEY` and `AWS_SECRET_ACCESS_KEY` are set correctly and have permission to access the required S3 buckets.
- **Pulsar connection issues:** Check that `PULSAR_URL` is set to the correct broker address and is reachable from your environment.
- **S3 upload or download failures:** Verify that `S3_BUCKET` (and any other relevant buckets like `PATCH_BUCKET` or `S3_SPDX_BUCKET`) exist, your credentials have the correct permissions, and the bucket region matches your configuration.

Check the application logs for detailed error messages.

## Release Process

The release process is fully automated and handled through GitHub Actions.
On every push to `main` or when a new tag is created, the following checks and steps are run automatically:

- QA checks (linting, formatting, type checking)
- Security scanning
- Unit tests
- Docker image build and push to the configured registry

Versioned releases are handled through the Releases page in github.

See [`.github/workflows/actions.yaml`](.github/workflows/actions.yaml) for details.

## License

This project is licensed under the United Kingdom Research and Innovation BSD Licence. See [LICENSE](LICENSE) for details.
