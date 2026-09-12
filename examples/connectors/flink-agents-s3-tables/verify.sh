#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

export AWS_REGION="${AWS_REGION:-us-east-1}"
export TABLE_BUCKET_ARN="$(terraform -chdir=terraform output -raw table_bucket_arn)"
eval "$(aws configure export-credentials --format env)"

echo "flink"
aws kinesisanalyticsv2 describe-application \
    --application-name "$(terraform -chdir=terraform output -raw application_name)" \
    --query 'ApplicationDetail.ApplicationStatus' --output text

echo
duckdb \
    -cmd "LOAD iceberg; LOAD httpfs;" \
    -cmd "CREATE SECRET aws (TYPE s3, PROVIDER credential_chain, CHAIN 'env', REGION '$AWS_REGION');" \
    -cmd "ATTACH '$TABLE_BUCKET_ARN' AS lake (TYPE iceberg, ENDPOINT_TYPE s3_tables);" \
    -c ".read sql/duckdb.sql"
