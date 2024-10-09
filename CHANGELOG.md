# Changelog

## v0.1.11 (09-10-2024)

- Update error handling for invalid links

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
