variable "aws_region" {
  description = "AWS region for all resources"
  type        = string
  default     = "us-west-2"
}

variable "aws_account_id" {
  description = "AWS account ID"
  type        = string
  default     = "018437500440"
}

variable "environment" {
  description = "Deployment environment (demo, staging, prod)"
  type        = string
  default     = "demo"
}

variable "project_name" {
  description = "Base name for all resources"
  type        = string
  default     = "crypto-surveillance"
}

variable "vpc_cidr" {
  description = "CIDR block for the VPC"
  type        = string
  default     = "10.100.0.0/16"
}

variable "private_subnet_cidrs" {
  description = "CIDR blocks for private subnets (one per AZ)"
  type        = list(string)
  default     = ["10.100.1.0/24", "10.100.2.0/24", "10.100.3.0/24"]
}

variable "public_subnet_cidrs" {
  description = "CIDR blocks for public subnets (NAT GW, ALB)"
  type        = list(string)
  default     = ["10.100.101.0/24", "10.100.102.0/24", "10.100.103.0/24"]
}

variable "kinesis_shard_count" {
  description = "Number of Kinesis shards per stream (1 shard = ~1 MB/s ingest)"
  type        = number
  default     = 2
}

variable "kinesis_retention_hours" {
  description = "Kinesis data retention period in hours (max 168 = 7 days)"
  type        = number
  default     = 24
}

variable "s3_raw_lifecycle_days" {
  description = "Days before raw objects transition to Glacier Instant Retrieval"
  type        = number
  default     = 30
}

variable "snowflake_external_id" {
  description = "External ID used in Snowflake Storage Integration (output from CREATE STORAGE INTEGRATION)"
  type        = string
  default     = "snowflake_storage_integration_external_id"
  sensitive   = true
}

variable "snowflake_iam_user_arn" {
  description = "Snowflake IAM user ARN (output from DESC STORAGE INTEGRATION)"
  type        = string
  default     = "arn:aws:iam::snowflake_account_id:user/snowflake"
  sensitive   = true
}

variable "snowflake_streaming_iam_user_arn" {
  description = "Snowflake IAM user ARN for Snowpipe Streaming / Kinesis connector"
  type        = string
  default     = "arn:aws:iam::snowflake_account_id:user/snowflake-streaming"
  sensitive   = true
}

variable "bedrock_model_id" {
  description = "Amazon Bedrock model ID for investigator copilot"
  type        = string
  default     = "anthropic.claude-3-5-sonnet-20241022-v2:0"
}

variable "lambda_runtime" {
  description = "Lambda runtime for normalizer functions"
  type        = string
  default     = "python3.12"
}

variable "enable_msk" {
  description = "Deploy MSK cluster in addition to Kinesis (higher throughput option)"
  type        = bool
  default     = false
}
