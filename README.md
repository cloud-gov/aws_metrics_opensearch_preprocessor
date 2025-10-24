## Overview
This repository contains tests for AWS Lambda functions that serve as preprocessors in the AWS OpenSearch pipeline. The Lambda functions have three primary purposes:

- **CloudWatch Metric Stream Transform**: Retrieve and attach relevant resource tags to metrics, filter sensitive data, and format metrics for downstream systems
- **CloudWatch Log Transform**: Add resource tags and filtering information to log data before processing
- **Log Group Subscription Manager**: Automatically assign subscription filters to newly created CloudWatch log groups for broker-created services

These Lambda functions run in AWS environments processing high-volume data streams and ensuring proper data enrichment and filtering before sending to S3 for ingestion into OpenSearch.

## Architecture
The preprocessor Lambda functions sit between CloudWatch services and the OpenSearch pipeline, acting as enrichment, filtering, and routing layers:

- **Metric Stream** → **Metric Transform Lambda** → **S3 Bucket** → **OpenSearch**
- **CloudWatch Logs** → **Log Transform Lambda** → **S3 Bucket** → **OpenSearch** 
- **New Log Groups** → **Subscription Manager Lambda** → **Configured Subscription Filters** → **S3 Bucket** → **OpenSearch** 

## What This Repository Tests

### Metric Stream Transform Lambda
- **Environment-Specific Processing**: Validates that the Lambda works correctly across different environments (dev, staging, prod)
- **Metric Processing**: Ensures expected metrics are processed and transformed correctly
- **Output Formatting**: Verifies that transformed data meets downstream OpenSearch requirements
- **Data Filtering**: Confirms sensitive information (account IDs, etc.) is properly removed before storage
- **Tag Enrichment**: Tests that appropriate resource tags are successfully attached to metrics

### CloudWatch Log Transform Lambda
- **Log Data Enrichment**: Validates that logs are properly enriched with resource tags and metadata
- **Log Filtering**: Ensures sensitive information is filtered from log entries before processing
- **Format Compatibility**: Verifies log data is formatted correctly for OpenSearch ingestion
- **Tag Enrichment**: Tests that appropriate resource tags are successfully attached to metrics


### Log Group Subscription Manager Lambda
- **Corrent filtering**: Tests that new CloudWatch log groups are properly filtered
- **Broker Service Identification**: Validates correct identification of broker-created services
- **Subscription Filter Assignment**: Ensures appropriate subscription filters are automatically configured
- **Error Handling**: Tests proper handling of edge cases and failure scenarios

## Testing Approach
- **Unit Tests**: Individual function testing for each Lambda
- **Integration Tests**: End-to-end pipeline testing across all three Lambda functions
- **Environment Validation**: Cross-environment testing (dev, staging, production)
- **Performance Testing**: Load testing for high-volume data processing scenarios
- **Error Recovery**: Testing failure modes and recovery mechanisms