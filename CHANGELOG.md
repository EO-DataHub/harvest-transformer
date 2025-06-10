# Changelog

## v0.1.19 (10-06-2025)

- Make better use of threading support
  - Use eodhp-utils with fixed threading support
  - Increase the requests connection pool used for talking to S3
  - Don't list the licence bucket on every message
- Update the licence and README

## v0.1.18 (28-03-2025)

- Command line options:
  - Add threading via the `-t` option
  - Set the verbosity of logging with `-v`

## v0.1.17 (21-03-2025)

- General upkeep
  - Prevent catalog_id from being updated for root catalogs, as this caused issues with internal harvests as IDs should be retained through transforms
  - Updates to handling of spdx license links to avoid errors

## v0.1.16 (07-03-2025)

- Use paths provided by harvesters

## v0.1.15 (18-02-2025)

- Quality of life code improvements
  - Remove conformance links from transformed links
  - Update license copy error handling to avoid throwing errors

## v0.1.14 (10-02-2025)

- Bugfixes and Code Improvements
  - Bump eodhp-utils version to address issues with S3 access timing
  - Added a patch mechanism to merge collections with patches as defined in a given S3 bucket
  - Override the method in the CatalogueChangeMessager class to ensure it supplies the correct bucket for bucket transactions.
  - Only need to update catalog_id for valid JSON files, parsed as dicts

## v0.1.13 (17-01-2025)

- Add bucket argument to Update File request to avoid use of default bucket in the Messager

## v0.1.12 (16-12-2024)

- Refactor to use eodhp-utils Messager Framework
  - Updated code to use Messager Framework defined in eodhp-utils
  - Updated tests to use same framework
  - Remove Pulsar dependency in tests

## v0.1.11 (09-10-2024)

- Update error handling for invalid links
- Add chunking to pulsar messages

## v0.1.10 (01-10-2024)

- Update error handling and retries
- Add render processor

## v0.1.9 (24-07-2024)

- Bugfix to correct Catalog ID update logic:
  - Catalog IDs only updated for the root catalog in the data selected for harvest
  - Addressed previous issue with non-root catalogs being updated with new IDs

## v0.1.8 (09-07-2024)

- Support nested catalog harvesting
  - Update to transform key logic to generate correct key structure for nested catalogs
  - Integration with stac-harvesters to generate keys from STAC URLs
  - Integration with STAC-FastApi ingester to provide correct STAC file keys
- Code cleanup
  - Update/add unit tests for new functions
  - Code restructure to support more granular unit testing

## v0.1.7 (02-07-2024)

- Ensure transformed files are output into the `transformed/` directory.

## v0.1.6 (21-05-2024)

- Move functionality into eodhp-utils

## v0.1.5 (21-05-2024)

- Bugfix for non-json data files:
  - Files will be uploaded as found with no transformation

## v0.1.4 (14-05-2024)

- Extend harvester identifier to rest of pulsar message

## v0.1.3 (14-05-2024)

- Add harvester identification via optional TOPIC environment variable

## v0.1.2 (07-05-2024)

- Refactored code to better separate S3/Pulsar functionality from transformer code
- Improved link correction in link processor
- Separated transformer code for different transformers
- Added tests for both link and workflow processors, including example STAC items

## v0.1.1 (01-05-2024)

- Added workflow transformer for STAC collections
  - Generate STAC collection definition for workflows
  - Add required fields for a collection
  - Scrape CWL script (if provided) to fill gaps in definition
  - Generate workflow ID (uuid) if not provided
  - Upload transformed workflow to S3 bucket
- Updated logging to streamline debug logs
- Added workflows
- Update dockerfile
- Changed AWS connection
- Parse direct URLs to upstream STAC catalogs
- All links within a file are updated, including nested links

## v0.1.0 (17-04-2024)

- Initial commit of Harvest Transformer
  - Pulsar subscriber to listen to a specific topic
  - Update all links within files described as added or updated to EODHP root
  - Manage transformed subdirectory within given S3 bucket
  - Pulsar producer to send list of updated files
  - Dockerfile to build module for deployment
