output "vpc_id" {
  description = "VPC ID"
  value       = aws_vpc.main.id
}

output "private_subnet_ids" {
  description = "Private subnet IDs"
  value       = aws_subnet.private[*].id
}

output "kinesis_stream_arns" {
  description = "ARNs of all CEX Kinesis streams (used in Snowflake integrations)"
  value       = { for k, v in aws_kinesis_stream.cex : k => v.arn }
}

output "kinesis_stream_names" {
  description = "Names of all CEX Kinesis streams"
  value       = { for k, v in aws_kinesis_stream.cex : k => v.name }
}

output "s3_raw_bucket_name" {
  description = "Raw landing S3 bucket name (used in Snowflake STORAGE INTEGRATION)"
  value       = aws_s3_bucket.raw.bucket
}

output "s3_raw_bucket_arn" {
  description = "Raw landing S3 bucket ARN"
  value       = aws_s3_bucket.raw.arn
}

output "s3_archive_bucket_name" {
  description = "Archive / DLQ S3 bucket name"
  value       = aws_s3_bucket.archive.bucket
}

output "snowflake_s3_role_arn" {
  description = "IAM role ARN for Snowflake S3 Storage Integration (paste into CREATE STORAGE INTEGRATION)"
  value       = aws_iam_role.snowflake_s3.arn
}

output "snowflake_kinesis_role_arn" {
  description = "IAM role ARN for Snowflake Snowpipe Streaming / Kinesis connector"
  value       = aws_iam_role.snowflake_kinesis.arn
}

output "snowflake_bedrock_role_arn" {
  description = "IAM role ARN for Snowflake External Access to Bedrock"
  value       = aws_iam_role.snowflake_bedrock.arn
}

output "snowpipe_sqs_arn" {
  description = "SQS queue ARN for Snowpipe event notifications (paste into CREATE PIPE)"
  value       = aws_sqs_queue.snowpipe_notifications.arn
}

output "kms_kinesis_key_arn" {
  description = "KMS key ARN for Kinesis encryption"
  value       = aws_kms_key.kinesis.arn
}

output "kms_s3_key_arn" {
  description = "KMS key ARN for S3 encryption"
  value       = aws_kms_key.s3.arn
}

output "lambda_onchain_indexer_arn" {
  description = "ARN of on-chain indexer Lambda"
  value       = aws_lambda_function.onchain_indexer.arn
}
