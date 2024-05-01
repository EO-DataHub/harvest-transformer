# Changelog

## 0.1.2 (01-05-2024)

- Added workflow transformer for STAC collections
  - Generate STAC collection definition for workflows
  - Add required fields for a collection
  - Scrape CWL script (if provided) to fill gaps in definition
  - Generate workflow ID (uuid) if not provided
  - Upload transformed workflow to S3 bucket
- Updated logging to streamline debug logs

## v0.1.1 (25-04-2024)

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
