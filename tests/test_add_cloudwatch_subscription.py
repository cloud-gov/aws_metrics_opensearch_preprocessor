import json
import base64
from unittest.mock import patch, MagicMock
import gzip
from botocore.stub import Stubber
import boto3
import pytest

from lambda_functions.add_cloudwatch_subscrition import (
    lambda_handler,
    make_prefixes
)
dummy_region = "us-gov-west-1"

class TestCloudwatchLambdaHandler:

    def test_lambda_handler_broker_logs(self, monkeypatch):
        """Test logs from broker"""
        # Sample cloudtrail data
        test_data = {
            "version": "0",
            "id": "82bc3140-3fa6-4afa-8c0f-93e83a425393",
            "detail-type": "AWS API Call via CloudTrail",
            "source": "aws.logs",
            "account": "123456789",
            "time": "2025-10-22T15:59:06Z",
            "region": "us-west-1",
            "resources": [],
            "detail": {
                "eventVersion": "1.11",
                "userIdentity": {
                    "type": "AssumedRole",
                    "principalId": "Noseforatude:random-175919",
                    "arn": "arn:aws:sts::1234556789:assumed-role/role/random-175919",
                    "accountId": "1234556789",
                    "accessKeyId": "WeAreWho1244",
                    "sessionContext": {
                        "sessionIssuer": {
                            "type": "Role",
                            "principalId": "Noseforatude",
                            "arn": "arn:aws:iam::1234556789:role/aws-service-role/rds.amazonaws.com/role",
                            "accountId": "1234556789",
                            "userName": "AWSServiceRoleForRDS"
                        },
                        "attributes": {
                            "creationDate": "2025-10-22T15:58:58Z",
                            "mfaAuthenticated": "false"
                        }
                    },
                    "invokedBy": "rds.amazonaws.com"
                },
                "eventTime": "2025-10-22T15:59:06Z",
                "eventSource": "logs.amazonaws.com",
                "eventName": "CreateLogGroup",
                "awsRegion": "us-gov-west-1",
                "sourceIPAddress": "rds.amazonaws.com",
                "userAgent": "rds.amazonaws.com",
                "requestParameters": {
                    "logGroupName": "/aws/rds/instance/cg-aws-broker-test"
                },
                "responseElements": 'null',
                "requestID": "5ac65be0-b8e0-42d5-91a6-a92b168d1729",
                "eventID": "ad11e4b2-e73b-48f3-8f59-227281cfa891",
                "readOnly": 'false',
                "eventType": "AwsApiCall",
                "apiVersion": "20140328",
                "managementEvent": 'true',
                "recipientAccountId": "1234567",
                "eventCategory": "Management"
            }
        }

        context = MagicMock()

        monkeypatch.setenv("FIREHOSE_ARN", "fireexample")
        monkeypatch.setenv("ROLE_ARN", "roleexample")

        # Create a stubbed logs client
        logs_client = boto3.client("logs", region_name=dummy_region)

        stubber = Stubber(logs_client)

        expected_param_for_stub = {
                "logGroupName":test_data["detail"]["requestParameters"]["logGroupName"],
                "filterName":"firehose_for_opensearch",
                "filterPattern":"",
                "destinationArn":"fireexample",
                "roleArn":"roleexample"
                }
        
        response = {}
        stubber.add_response(
            "put_subscription_filter", response, expected_param_for_stub
        )
        stubber.activate()


        with patch("lambda_functions.add_cloudwatch_subscrition.make_prefixes",
            return_value="cg-aws-broker-test",
        ),patch(
            "boto3.client", return_value=logs_client
        ):
            result = lambda_handler(test_data,context)

        assert result == 0

    def test_lambda_handler_not_broker_logs(self, monkeypatch):
        """Test logs from broker"""
        # Sample cloudtrail data
        test_data = {
            "version": "0",
            "id": "82bc3140-3fa6-4afa-8c0f-93e83a425393",
            "detail-type": "AWS API Call via CloudTrail",
            "source": "aws.logs",
            "account": "123456789",
            "time": "2025-10-22T15:59:06Z",
            "region": "us-west-1",
            "resources": [],
            "detail": {
                "eventVersion": "1.11",
                "userIdentity": {
                    "type": "AssumedRole",
                    "principalId": "Noseforatude:random-175919",
                    "arn": "arn:aws:sts::1234556789:assumed-role/role/random-175919",
                    "accountId": "1234556789",
                    "accessKeyId": "WeAreWho1244",
                    "sessionContext": {
                        "sessionIssuer": {
                            "type": "Role",
                            "principalId": "Noseforatude",
                            "arn": "arn:aws:iam::1234556789:role/aws-service-role/rds.amazonaws.com/role",
                            "accountId": "1234556789",
                            "userName": "AWSServiceRoleForRDS"
                        },
                        "attributes": {
                            "creationDate": "2025-10-22T15:58:58Z",
                            "mfaAuthenticated": "false"
                        }
                    },
                    "invokedBy": "rds.amazonaws.com"
                },
                "eventTime": "2025-10-22T15:59:06Z",
                "eventSource": "logs.amazonaws.com",
                "eventName": "CreateLogGroup",
                "awsRegion": "us-gov-west-1",
                "sourceIPAddress": "rds.amazonaws.com",
                "userAgent": "rds.amazonaws.com",
                "requestParameters": {
                    "logGroupName": "/aws/rds/instance/apple"
                },
                "responseElements": 'null',
                "requestID": "5ac65be0-b8e0-42d5-91a6-a92b168d1729",
                "eventID": "ad11e4b2-e73b-48f3-8f59-227281cfa891",
                "readOnly": 'false',
                "eventType": "AwsApiCall",
                "apiVersion": "20140328",
                "managementEvent": 'true',
                "recipientAccountId": "1234567",
                "eventCategory": "Management"
            }
        }

        context = MagicMock()

        monkeypatch.setenv("FIREHOSE_ARN", "fireexample")
        monkeypatch.setenv("ROLE_ARN", "roleexample")

        # Create a stubbed logs client
        logs_client = boto3.client("logs", region_name=dummy_region)

        stubber = Stubber(logs_client)

        expected_param_for_stub = {
                "logGroupName":test_data["detail"]["requestParameters"]["logGroupName"],
                "filterName":"firehose_for_opensearch",
                "filterPattern":"",
                "destinationArn":"fireexample",
                "roleArn":"roleexample"
                }
        
        response = {}
        stubber.add_response(
            "put_subscription_filter", response, expected_param_for_stub
        )
        stubber.activate()


        with patch("lambda_functions.add_cloudwatch_subscrition.make_prefixes",
            return_value="cg-aws-broker-test",
        ),patch(
            "boto3.client", return_value=logs_client
        ):
            result = lambda_handler(test_data,context)

        # means it did not work
        assert result == 1


    @pytest.mark.parametrize(
        "environment, expected_rds_prefix",
        [
            pytest.param("development", "cg-aws-broker-dev"),
            pytest.param("staging", "cg-aws-broker-stage"),
            pytest.param("production", "cg-aws-broker-prod"),
        ],
    )
    def test_environment_cloudwatch_rds_success(
        self,
        monkeypatch,
        environment,
        expected_rds_prefix,
    ):
        monkeypatch.setenv("FIREHOSE_ARN", "fireexample")
        monkeypatch.setenv("ROLE_ARN", "roleexample")

        monkeypatch.setenv("ENVIRONMENT", environment)

        rds_prefix = make_prefixes()
        assert rds_prefix == expected_rds_prefix

        """Test that environment only accepts environment prefix that match environment"""
        test_data = {
            "version": "0",
            "id": "82bc3140-3fa6-4afa-8c0f-93e83a425393",
            "detail-type": "AWS API Call via CloudTrail",
            "source": "aws.logs",
            "account": "123456789",
            "time": "2025-10-22T15:59:06Z",
            "region": "us-west-1",
            "resources": [],
            "detail": {
                "eventVersion": "1.11",
                "userIdentity": {
                    "type": "AssumedRole",
                    "principalId": "Noseforatude:random-175919",
                    "arn": "arn:aws:sts::1234556789:assumed-role/role/random-175919",
                    "accountId": "1234556789",
                    "accessKeyId": "WeAreWho1244",
                    "sessionContext": {
                        "sessionIssuer": {
                            "type": "Role",
                            "principalId": "Noseforatude",
                            "arn": "arn:aws:iam::1234556789:role/aws-service-role/rds.amazonaws.com/role",
                            "accountId": "1234556789",
                            "userName": "AWSServiceRoleForRDS"
                        },
                        "attributes": {
                            "creationDate": "2025-10-22T15:58:58Z",
                            "mfaAuthenticated": "false"
                        }
                    },
                    "invokedBy": "rds.amazonaws.com"
                },
                "eventTime": "2025-10-22T15:59:06Z",
                "eventSource": "logs.amazonaws.com",
                "eventName": "CreateLogGroup",
                "awsRegion": "us-gov-west-1",
                "sourceIPAddress": "rds.amazonaws.com",
                "userAgent": "rds.amazonaws.com",
                "requestParameters": {
                    "logGroupName": f"/aws/rds/instance/{expected_rds_prefix}"
                },
                "responseElements": 'null',
                "requestID": "5ac65be0-b8e0-42d5-91a6-a92b168d1729",
                "eventID": "ad11e4b2-e73b-48f3-8f59-227281cfa891",
                "readOnly": 'false',
                "eventType": "AwsApiCall",
                "apiVersion": "20140328",
                "managementEvent": 'true',
                "recipientAccountId": "1234567",
                "eventCategory": "Management"
            }
        }

        # Create a stubbed logs client
        logs_client = boto3.client("logs", region_name=dummy_region)

        stubber = Stubber(logs_client)

        expected_param_for_stub = {
                "logGroupName":test_data["detail"]["requestParameters"]["logGroupName"],
                "filterName":"firehose_for_opensearch",
                "filterPattern":"",
                "destinationArn":"fireexample",
                "roleArn":"roleexample"
                }
        
        response = {}
        stubber.add_response(
            "put_subscription_filter", response, expected_param_for_stub
        )
        stubber.activate()

        context = MagicMock()

        with patch("lambda_functions.transform_lambda.logger"), patch(
            "boto3.client", return_value=logs_client
        ):
            result = lambda_handler(test_data,context)

        assert result == 0



    @pytest.mark.parametrize(
        "environment, expected_rds_prefix",
        [
            pytest.param("development", "cg-aws-broker-prod"),
            pytest.param("staging", "cg-aws-broker-prod"),
            pytest.param("production", "cg-aws-broker-stage"),
        ],
    )
    def test_environment_cloudwatch_rds_failure(
        self,
        monkeypatch,
        environment,
        expected_rds_prefix,
    ):
        monkeypatch.setenv("FIREHOSE_ARN", "fireexample")
        monkeypatch.setenv("ROLE_ARN", "roleexample")

        monkeypatch.setenv("ENVIRONMENT", environment)

        rds_prefix = make_prefixes()
        assert rds_prefix != expected_rds_prefix

        """Test that environment only accepts environment prefix that match environment"""
        test_data = {
            "version": "0",
            "id": "82bc3140-3fa6-4afa-8c0f-93e83a425393",
            "detail-type": "AWS API Call via CloudTrail",
            "source": "aws.logs",
            "account": "123456789",
            "time": "2025-10-22T15:59:06Z",
            "region": "us-west-1",
            "resources": [],
            "detail": {
                "eventVersion": "1.11",
                "userIdentity": {
                    "type": "AssumedRole",
                    "principalId": "Noseforatude:random-175919",
                    "arn": "arn:aws:sts::1234556789:assumed-role/role/random-175919",
                    "accountId": "1234556789",
                    "accessKeyId": "WeAreWho1244",
                    "sessionContext": {
                        "sessionIssuer": {
                            "type": "Role",
                            "principalId": "Noseforatude",
                            "arn": "arn:aws:iam::1234556789:role/aws-service-role/rds.amazonaws.com/role",
                            "accountId": "1234556789",
                            "userName": "AWSServiceRoleForRDS"
                        },
                        "attributes": {
                            "creationDate": "2025-10-22T15:58:58Z",
                            "mfaAuthenticated": "false"
                        }
                    },
                    "invokedBy": "rds.amazonaws.com"
                },
                "eventTime": "2025-10-22T15:59:06Z",
                "eventSource": "logs.amazonaws.com",
                "eventName": "CreateLogGroup",
                "awsRegion": "us-gov-west-1",
                "sourceIPAddress": "rds.amazonaws.com",
                "userAgent": "rds.amazonaws.com",
                "requestParameters": {
                    "logGroupName": f"/aws/rds/instance/{expected_rds_prefix}"
                },
                "responseElements": 'null',
                "requestID": "5ac65be0-b8e0-42d5-91a6-a92b168d1729",
                "eventID": "ad11e4b2-e73b-48f3-8f59-227281cfa891",
                "readOnly": 'false',
                "eventType": "AwsApiCall",
                "apiVersion": "20140328",
                "managementEvent": 'true',
                "recipientAccountId": "1234567",
                "eventCategory": "Management"
            }
        }

        # Create a stubbed logs client
        logs_client = boto3.client("logs", region_name=dummy_region)

        stubber = Stubber(logs_client)

        expected_param_for_stub = {
                "logGroupName":test_data["detail"]["requestParameters"]["logGroupName"],
                "filterName":"firehose_for_opensearch",
                "filterPattern":"",
                "destinationArn":"fireexample",
                "roleArn":"roleexample"
                }
        
        response = {}
        stubber.add_response(
            "put_subscription_filter", response, expected_param_for_stub
        )
        stubber.activate()

        context = MagicMock()

        with patch("lambda_functions.transform_lambda.logger"), patch(
            "boto3.client", return_value=logs_client
        ):
            result = lambda_handler(test_data,context)

        assert result == 1
