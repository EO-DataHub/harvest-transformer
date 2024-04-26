# Changelog

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
