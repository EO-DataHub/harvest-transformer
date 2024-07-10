# harvest-transformer

A service to transform harvested metadata. This has the following responsibilities:

- Await pulsar messages from the "harvested" topic, listing files ready for transform.
- Update file contents to refer to a output_url given as a command line argument, and target provided in the incoming message.
- Remove file contents that are not needed in the EODHP system.
- Generate/complete STAC Collection definitions for workflows, including scraping of CWL script, if provided, to complete blank fields
- Upload transformed files to S3 under a transformed prefix.
- Send a pulsar message to the "transformed" topic, listing the transformed files.

## Installation

1. Follow the instructions [here](https://github.com/UKEODHP/template-python/blob/main/README.md) for installing a
   Python 3.11 environment.
2. Install dependencies:

```commandline
make setup
```

3. Set up environment variables

```commandline
export PULSAR_URL=<pulsar_url>
export AWS_ACCESS_KEY=<aws_access_key>
export AWS_SECRET_ACCESS_KEY=<aws_secret_access_key>
```

4. Run the service

```commandline
./venv/bin/python -m harvest_transformer <output_url>
```

## Building and testing

This component uses `pytest` tests and the `ruff` and `black` linters. `black` will reformat your code in an
opinionated way.

A number of `make` targets are defined:

- `make setup`: ensure requirements.txt is up-to-date and set up or update a dev environment (safe to run repeatedly)
- `make test`: run tests continuously
- `make testonce`: run tests once
- `make lint`: lint and reformat
- `make requirements`: Update requirements.txt and requirements-dev.txt from pyproject.toml
- `make requirements`: Like `make requirements` but uses `-U` to update to the latest allowed version of everything
- `make dockerbuild`: build a `latest` Docker image (use `make dockerbuild `VERSION=1.2.3` for a release image)
- `make dockerpush`: push a `latest` Docker image (again, you can add `VERSION=1.2.3`) - normally this should be done
  only via the build system and its GitHub actions.
