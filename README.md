# harvest-transformer
A service to transform harvested metadata. This has the following responsibilities:
- Await pulsar messages from the "harvested" topic, listing files ready for transform.
- Update file contents to refer to a output_url given as a command line argument, and target provided in the incoming message.
- Remove file contents that are not needed in the EODHP system.
- Upload transformed files to S3 under a transformed prefix.
- Send a pulsar message to the "transformed" topic, listing the transformed files.


## Installation

1. Follow the instructions [here](https://github.com/UKEODHP/template-python/blob/main/README.md) for installing a 
Python 3.11 environment.
2. Install dependencies:

```commandline
pip3 install -r eodhp_web_presence/requirements.txt
```
3. Set up environment variables
```commandline
export PULSAR_URL=<pulsar_url>
export AWS_ACCESS_KEY=<aws_access_key>
export AWS_SECRET_ACCESS_KEY=<aws_secret_access_key>
```
4. Run the service
```commandline
python -m harvest_transformer <output_url>
```
