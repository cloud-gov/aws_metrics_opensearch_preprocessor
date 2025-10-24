import boto3
import gzip
import json
import os
import logging
from functools import lru_cache
import base64

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    """
    This function processes CloudWatch Logs from Firehose.
    """
    output_records = []
    try:
        rds_prefix = make_prefixes()
        region = boto3.Session().region_name or os.environ.get("AWS_REGION")
        account_id = os.environ.get("ACCOUNT_ID")
        if not account_id:
            raise ValueError("ACCOUNT_ID environment variable is required")

        rds_client = boto3.client("rds", region_name=region)
    except Exception as e:
        logger.error(f"Initialization error: {str(e)}")
        # this will store the records in the s3 bucket as untransformable for later retrying
        return {"records": []}

    try:
        for record in event["records"]:
            pre_json_value = gzip.decompress(base64.b64decode(record["data"]))
            processed_logs = []
            for line in pre_json_value.strip().splitlines():
                logs = json.loads(line)
                log_results = process_logs(
                    logs, rds_client, region, account_id, rds_prefix
                )
                if log_results:
                    processed_logs.extend(log_results)

            if processed_logs:
                # Create newline-delimited JSON (no compression)
                output_data = (
                    "\n".join([json.dumps(log) for log in processed_logs]) + "\n"
                )

                # Just base64 encode for Firehose transport (no gzip)
                encoded_output = base64.b64encode(output_data.encode("utf-8")).decode(
                    "utf-8"
                )

                output_record = {
                    "recordId": record["recordId"],
                    "result": "Ok",
                    "data": encoded_output,
                }
                output_records.append(output_record)
            else:
                output_record = {
                    "recordId": record["recordId"],
                    "result": "Dropped",
                    "data": record["data"],
                }
                output_records.append(output_record)
            logger.info(f"Processed record with {len(processed_logs)} logs")
    except Exception as e:
        logger.error(f"Error processing logs: {str(e)}")
    return {"records": output_records}


def make_prefixes():
    environment = os.getenv("ENVIRONMENT")
    if not environment:
        raise RuntimeError("environment is required")

    rds_prefix = "cg-aws-broker-"
    environment_suffixes = {
        "production": "prod",
        "staging": "stage",
        "development": "dev",
    }
    if environment not in environment_suffixes:
        raise RuntimeError(f"environment is invalid: {environment}")

    rds_prefix += environment_suffixes[environment]

    return rds_prefix


def process_logs(logs, client, region, account_id, rds_prefix):
    try:
        return_logs = []
        resource_name = logs["logGroup"].split("/")[4]

        tags = get_resource_tags_from_log(
            resource_name, client, region, account_id, rds_prefix
        )

        if len(tags.keys()) > 0:
            for event in logs["logEvents"]:
                entry = {
                    "logGroup": logs["logGroup"],
                    "logStream": logs["logStream"],
                    "message": event["message"],
                    "timestamp": event["timestamp"],
                    "Tags": tags,
                }
                return_logs.append(entry)
        else:
            return None
        return return_logs

    except Exception as e:
        logger.error(f"Could not process logs: {e}")
        return None


def get_resource_tags_from_log(
    resource_name, client, region, account_id, rds_prefix
) -> dict:
    tags = {}
    try:
        if resource_name is not None and resource_name.startswith(rds_prefix):
            arn = f"arn:aws-us-gov:rds:{region}:{account_id}:db:{resource_name}"
            tags = get_tags_from_arn(arn, client)
    except Exception as e:
        logger.error(f"Error with getting tags for resource: {e}")
    return tags


@lru_cache(maxsize=256)
def get_tags_from_arn(arn, client) -> dict:
    tags = {}
    if ":db:" in arn:
        try:
            response = client.list_tags_for_resource(ResourceName=arn)
            tags = {tag["Key"]: tag["Value"] for tag in response.get("TagList", [])}
            if "Organization GUID" not in tags:
                return {}
        except Exception as e:
            logger.error(f"Could not fetch tags: {e}")
    return tags
