# aws metrics opensearch preprocessor pipeline

A testing repository for AWS Lambda functions that transform CloudWatch metric streams by enriching metrics with resource tags and filtering information.

## Overview

This repository contains tests for a Lambda function that serves as a transform for AWS CloudWatch metric streams. The Lambda's primary purposes are:

- **Tag Enrichment**: Retrieve and attach relevant resource tags to metrics
- **Data Filtering**: Remove sensitive fields (account IDs, etc.) before sending data downstream
- **Data Formatting**: Ensure metrics are properly formatted for downstream systems

The Lambda runs approximately every minute in AWS environments and processes high-volume metric data streams.

## Architecture

The transform Lambda sits between the metric stream and downstream processing (s3 bucket), acting as a filter and enrichment layer.

## What This Repository Tests

- **Environment-Specific Processing**: Validates that the Lambda works correctly across different environments (dev,staging,prod)
- **Metric Processing**: Ensures expected metrics are processed and transformed correctly
- **Output Formatting**: Verifies that transformed data meets downstream system requirements for formatting
- **Data Filtering**: Confirms information is properly removed before being stored/
- **Tag Enrichment**: Tests that appropriate resource tags are successfully attached