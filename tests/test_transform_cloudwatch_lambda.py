import json
import base64
from unittest.mock import patch, MagicMock
import gzip
from botocore.stub import Stubber
import boto3
import pytest

from lambda_functions.transform_cloudwatch_lambda import (
    lambda_handler,
    make_prefixes,
    get_resource_tags_from_log,
)

dummy_region = "us-gov-west-1"


class TestLambdaHandler:

    def test_lambda_handler_single_log_line(self, monkeypatch):
        """Test processing a single log line"""
        # Sample log data as newline-delimited JSON
        log_data = {
            "messageType": "DATA_MESSAGE",
            "owner": "12345678910",
            "logGroup": "/aws/rds/instance/cg-aws-broker-devtest/postgresql",
            "logStream": "cg-aws-broker-devtest.0",
            "subscriptionFilters": ["testing"],
            "logEvents": [
                {
                    "id": "12345678912345678901234567890123456789123456789012345670",
                    "timestamp": 1759774467000,
                    "message": "This is a test",
                },
            ],
        }

        mock_tags = {"Environment": "production", "Owner": "team-alpha"}

        # Create newline-delimited JSON
        ndjson_data = json.dumps(log_data) + "\n"
        compressed_data = gzip.compress(ndjson_data.encode("utf-8"))
        encoded_data = base64.b64encode(compressed_data).decode("utf-8")

        event = {"records": [{"recordId": "test-record-1", "data": encoded_data}]}

        context = MagicMock()

        monkeypatch.setenv("AWS_REGION", "us-gov-west-1")
        monkeypatch.setenv("ACCOUNT_ID", "123456")

        with patch("lambda_functions.transform_cloudwatch_lambda.logger"), patch(
            "lambda_functions.transform_cloudwatch_lambda.get_resource_tags_from_log",
            return_value=mock_tags,
        ):
            # Set up the mock return value
            result = lambda_handler(event, context)
        # Assertions
        assert "records" in result
        assert len(result["records"]) == 1
        assert result["records"][0]["recordId"] == "test-record-1"
        assert result["records"][0]["result"] == "Ok"

        # Decode and verify output
        output_data = base64.b64decode(result["records"][0]["data"]).decode("utf-8")
        output_logs = [json.loads(line) for line in output_data.strip().split("\n")]

        assert len(output_logs) == 1
        log = output_logs[0]

        # Verify removed fields
        assert "owner" not in log
        assert "subscriptionFilters" not in log
        assert "messageType" not in log

        # Verify core data is preserved
        assert log["logGroup"] == "/aws/rds/instance/cg-aws-broker-devtest/postgresql"
        assert log["logStream"] == "cg-aws-broker-devtest.0"
        assert log["message"] == "This is a test"

        assert log["Tags"]["Environment"] == "production"
        assert log["Tags"]["Owner"] == "team-alpha"

    def test_lambda_handler_multiple_log_lines(self, monkeypatch):
        """Test processing multiple log lines in one record, should seperate different events"""
        log_data = {
            "messageType": "DATA_MESSAGE",
            "owner": "12345678910",
            "logGroup": "/aws/rds/instance/cg-aws-broker-devtest/postgresql",
            "logStream": "cg-aws-broker-devtest.0",
            "subscriptionFilters": ["testing"],
            "logEvents": [
                {
                    "id": "12345678912345678901234567890123456789123456789012345670",
                    "timestamp": 1759774467000,
                    "message": "This is a test",
                },
                {
                    "id": "12345678912345678901234567890123456789123456789012345670",
                    "timestamp": 1759774467002,
                    "message": "do you like my test",
                },
            ],
        }
        mock_tags = {"Environment": "production", "Owner": "team-alpha"}

        # Create newline-delimited JSON
        ndjson_data = json.dumps(log_data) + "\n"
        compressed_data = gzip.compress(ndjson_data.encode("utf-8"))
        encoded_data = base64.b64encode(compressed_data).decode("utf-8")

        event = {"records": [{"recordId": "multi-log-record", "data": encoded_data}]}

        context = MagicMock()

        monkeypatch.setenv("AWS_REGION", "us-gov-west-1")
        monkeypatch.setenv("ACCOUNT_ID", "123456")
        with patch("lambda_functions.transform_cloudwatch_lambda.logger"), patch(
            "lambda_functions.transform_cloudwatch_lambda.get_resource_tags_from_log",
            return_value=mock_tags,
        ):
            result = lambda_handler(event, context)

        assert len(result["records"]) == 1
        assert result["records"][0]["result"] == "Ok"

        # Decode and verify multiple logs
        output_data = base64.b64decode(result["records"][0]["data"]).decode("utf-8")
        output_logs = [json.loads(line) for line in output_data.strip().split("\n")]

        assert len(output_logs) == 2
        assert output_logs[0]["message"] == "This is a test"
        assert output_logs[1]["message"] == "do you like my test"
        assert output_logs[0]["timestamp"] == 1759774467000
        assert output_logs[1]["timestamp"] == 1759774467002
        assert output_logs[0]["Tags"]["Environment"] == "production"
        assert output_logs[0]["Tags"]["Owner"] == "team-alpha"
        assert output_logs[1]["Tags"]["Environment"] == "production"
        assert output_logs[1]["Tags"]["Owner"] == "team-alpha"

    @pytest.mark.parametrize(
        "environment, expected_rds_prefix",
        [
            pytest.param("development", "cg-aws-broker-dev"),
            pytest.param("staging", "cg-aws-broker-stage"),
            pytest.param("production", "cg-aws-broker-prod"),
        ],
    )
    def test_get_resource_tags_from_metric_rds_success(
        self,
        monkeypatch,
        environment,
        expected_rds_prefix,
    ):
        monkeypatch.setenv("AWS_REGION", "us-gov-west-1")
        monkeypatch.setenv("ACCOUNT_ID", "123456")
        monkeypatch.setenv("ENVIRONMENT", environment)
        monkeypatch.setenv("CLIENT", "123456")

        rds_prefix = make_prefixes()
        assert rds_prefix == expected_rds_prefix

        """Test that environment only accepts environment prefix that match environment"""
        log_data = {
            "messageType": "DATA_MESSAGE",
            "owner": "12345678910",
            "logGroup": f"/aws/rds/instance/{rds_prefix}-test/postgresql",
            "logStream": "cg-aws-broker-devtest.0",
            "subscriptionFilters": ["testing"],
            "logEvents": [
                {
                    "id": "12345678912345678901234567890123456789123456789012345670",
                    "timestamp": 1759774467000,
                    "message": "This is a test",
                },
                {
                    "id": "12345678912345678901234567890123456789123456789012345670",
                    "timestamp": 1759774467002,
                    "message": "do you like my test",
                },
            ],
        }

        # Create a stubbed rds client
        rds_client = boto3.client("rds", region_name=dummy_region)

        stubber = Stubber(rds_client)
        resource_name = log_data["logGroup"].split("/")[4]
        fake_arn = f"arn:aws-us-gov:rds:us-gov-west-1:123456:db:{resource_name}"

        fake_tags = {
            "TagList": [
                {"Key": "Environment", "Value": environment},
                {"Key": "Testing", "Value": "enabled"},
                {"Key": "Organization GUID", "Value": "cloudgovtests"},
            ]
        }

        expected_param_for_stub = {"ResourceName": fake_arn}
        stubber.add_response(
            "list_tags_for_resource", fake_tags, expected_param_for_stub
        )
        stubber.activate()

        with patch("lambda_functions.transform_lambda.logger"), patch(
            "boto3.client", return_value=rds_client
        ):
            result = get_resource_tags_from_log(
                resource_name, rds_client, dummy_region, 123456, rds_prefix
            )

        # if tags are returned environment is correct
        assert result["Environment"] == environment
        assert result["Testing"] == "enabled"
        assert result["Organization GUID"] == "cloudgovtests"

    @pytest.mark.parametrize(
        "environment, expected_rds_prefix",
        [
            pytest.param("development", "cg-aws-broker-prod"),
            pytest.param("staging", "cg-aws-broker-prod"),
            pytest.param("production", "cg-aws-broker-stage"),
        ],
    )
    def test_get_resource_tags_from_metric_rds_failure(
        self,
        monkeypatch,
        environment,
        expected_rds_prefix,
    ):
        monkeypatch.setenv("AWS_REGION", "us-gov-west-1")
        monkeypatch.setenv("ACCOUNT_ID", "123456")
        monkeypatch.setenv("ENVIRONMENT", environment)
        monkeypatch.setenv("CLIENT", "123456")

        rds_prefix = make_prefixes()
        assert rds_prefix != expected_rds_prefix

        """Test that environment only accepts environment prefix that match environment"""
        log_data = {
            "messageType": "DATA_MESSAGE",
            "owner": "12345678910",
            "logGroup": f"/aws/rds/instance/{rds_prefix}-test/postgresql",
            "logStream": "cg-aws-broker-devtest.0",
            "subscriptionFilters": ["testing"],
            "logEvents": [
                {
                    "id": "12345678912345678901234567890123456789123456789012345670",
                    "timestamp": 1759774467000,
                    "message": "This is a test",
                },
                {
                    "id": "12345678912345678901234567890123456789123456789012345670",
                    "timestamp": 1759774467002,
                    "message": "do you like my test",
                },
            ],
        }

        # Create a stubbed rds client
        rds_client = boto3.client("rds", region_name=dummy_region)

        stubber = Stubber(rds_client)
        resource_name = log_data["logGroup"].split("/")[4]
        fake_arn = f"arn:aws-us-gov:rds:us-gov-west-1:123456:db:{resource_name}"

        fake_tags = {
            "TagList": [
                {"Key": "Environment", "Value": environment},
                {"Key": "Testing", "Value": "enabled"},
                {"Key": "Organization GUID", "Value": "cloudgovtests"},
            ]
        }

        expected_param_for_stub = {"ResourceName": fake_arn}
        stubber.add_response(
            "list_tags_for_resource", fake_tags, expected_param_for_stub
        )
        stubber.activate()

        with patch("lambda_functions.transform_lambda.logger"), patch(
            "boto3.client", return_value=rds_client
        ):
            result = get_resource_tags_from_log(
                resource_name, rds_client, dummy_region, 123456, expected_rds_prefix
            )

        assert result == {}
