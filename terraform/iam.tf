# ─── Snowflake Storage Integration Role ───────────────────────────────────────
# Allow Snowflake to read/list the raw S3 bucket for Snowpipe batch ingest.
# After CREATE STORAGE INTEGRATION in Snowflake, update snowflake_external_id
# and snowflake_iam_user_arn variables with values from DESC INTEGRATION.

resource "aws_iam_role" "snowflake_s3" {
  name        = "${var.project_name}-snowflake-s3-role"
  description = "Allows Snowflake Storage Integration to access raw S3 bucket"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = { AWS = var.snowflake_iam_user_arn }
        Action    = "sts:AssumeRole"
        Condition = {
          StringEquals = { "sts:ExternalId" = var.snowflake_external_id }
        }
      }
    ]
  })

  tags = { Name = "${var.project_name}-snowflake-s3-role" }
}

resource "aws_iam_role_policy" "snowflake_s3" {
  name = "${var.project_name}-snowflake-s3-policy"
  role = aws_iam_role.snowflake_s3.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:GetObjectVersion",
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]
        Resource = [
          aws_s3_bucket.raw.arn,
          "${aws_s3_bucket.raw.arn}/*"
        ]
      },
      {
        Effect   = "Allow"
        Action   = ["kms:Decrypt", "kms:DescribeKey"]
        Resource = aws_kms_key.s3.arn
      }
    ]
  })
}

# ─── Snowpipe Streaming / Kinesis Connector Role ──────────────────────────────
# Allows Snowflake Kafka Connector (or Snowpipe Streaming SDK) to read Kinesis.

resource "aws_iam_role" "snowflake_kinesis" {
  name        = "${var.project_name}-snowflake-kinesis-role"
  description = "Allows Snowflake Snowpipe Streaming to read Kinesis Data Streams"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = { AWS = var.snowflake_streaming_iam_user_arn }
        Action    = "sts:AssumeRole"
        Condition = {
          StringEquals = { "sts:ExternalId" = var.snowflake_external_id }
        }
      }
    ]
  })

  tags = { Name = "${var.project_name}-snowflake-kinesis-role" }
}

resource "aws_iam_role_policy" "snowflake_kinesis" {
  name = "${var.project_name}-snowflake-kinesis-policy"
  role = aws_iam_role.snowflake_kinesis.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "kinesis:GetRecords",
          "kinesis:GetShardIterator",
          "kinesis:DescribeStream",
          "kinesis:DescribeStreamSummary",
          "kinesis:ListStreams",
          "kinesis:ListShards"
        ]
        Resource = [for s in aws_kinesis_stream.cex : s.arn]
      },
      {
        Effect   = "Allow"
        Action   = ["kms:Decrypt", "kms:DescribeKey"]
        Resource = aws_kms_key.kinesis.arn
      }
    ]
  })
}

# ─── Lambda Execution Role ─────────────────────────────────────────────────────
resource "aws_iam_role" "lambda_exec" {
  name        = "${var.project_name}-lambda-exec-role"
  description = "Execution role for on-chain indexer and log normalizer Lambdas"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = { Service = "lambda.amazonaws.com" }
        Action    = "sts:AssumeRole"
      }
    ]
  })

  tags = { Name = "${var.project_name}-lambda-exec-role" }
}

resource "aws_iam_role_policy_attachment" "lambda_vpc" {
  role       = aws_iam_role.lambda_exec.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole"
}

resource "aws_iam_role_policy" "lambda_kinesis_s3" {
  name = "${var.project_name}-lambda-kinesis-s3-policy"
  role = aws_iam_role.lambda_exec.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "kinesis:PutRecord",
          "kinesis:PutRecords",
          "kinesis:DescribeStream"
        ]
        Resource = [for s in aws_kinesis_stream.cex : s.arn]
      },
      {
        Effect = "Allow"
        Action = ["s3:PutObject", "s3:PutObjectAcl"]
        Resource = "${aws_s3_bucket.raw.arn}/*"
      },
      {
        Effect   = "Allow"
        Action   = ["kms:GenerateDataKey", "kms:Decrypt"]
        Resource = [aws_kms_key.kinesis.arn, aws_kms_key.s3.arn]
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:${var.aws_region}:${var.aws_account_id}:log-group:/aws/lambda/${var.project_name}-*"
      },
      {
        Effect   = "Allow"
        Action   = ["ssm:GetParameter"]
        Resource = "arn:aws:ssm:${var.aws_region}:${var.aws_account_id}:parameter/${var.project_name}/*"
      }
    ]
  })
}

# ─── Firehose Role ─────────────────────────────────────────────────────────────
resource "aws_iam_role" "firehose" {
  name = "${var.project_name}-firehose-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = { Service = "firehose.amazonaws.com" }
        Action    = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role_policy" "firehose_s3" {
  name = "${var.project_name}-firehose-s3-policy"
  role = aws_iam_role.firehose.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:AbortMultipartUpload",
          "s3:GetBucketLocation",
          "s3:GetObject",
          "s3:ListBucket",
          "s3:ListBucketMultipartUploads",
          "s3:PutObject"
        ]
        Resource = [aws_s3_bucket.raw.arn, "${aws_s3_bucket.raw.arn}/*"]
      },
      {
        Effect   = "Allow"
        Action   = ["kms:GenerateDataKey", "kms:Decrypt"]
        Resource = aws_kms_key.s3.arn
      },
      {
        Effect   = "Allow"
        Action   = ["logs:PutLogEvents"]
        Resource = "${aws_cloudwatch_log_group.firehose.arn}:*"
      }
    ]
  })
}

# ─── CloudWatch → Firehose subscription role ──────────────────────────────────
resource "aws_iam_role" "cloudwatch_to_firehose" {
  name = "${var.project_name}-cw-firehose-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = { Service = "logs.${var.aws_region}.amazonaws.com" }
        Action    = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role_policy" "cloudwatch_to_firehose" {
  name = "${var.project_name}-cw-firehose-policy"
  role = aws_iam_role.cloudwatch_to_firehose.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["firehose:PutRecord", "firehose:PutRecordBatch"]
        Resource = aws_kinesis_firehose_delivery_stream.logs_to_s3.arn
      }
    ]
  })
}

# ─── Bedrock invocation policy (attached to Lambda if needed) ─────────────────
resource "aws_iam_policy" "bedrock_invoke" {
  name        = "${var.project_name}-bedrock-invoke"
  description = "Allow invoking Bedrock models for investigator copilot"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["bedrock:InvokeModel", "bedrock:InvokeModelWithResponseStream"]
        Resource = "arn:aws:bedrock:${var.aws_region}::foundation-model/${var.bedrock_model_id}"
      }
    ]
  })
}

# Snowflake calls Bedrock via External Access (not Lambda) — attach this policy
# to a dedicated IAM role that Snowflake's External Access Integration assumes.
resource "aws_iam_role" "snowflake_bedrock" {
  name        = "${var.project_name}-snowflake-bedrock-role"
  description = "Allows Snowflake External Access to call Amazon Bedrock"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = { AWS = var.snowflake_streaming_iam_user_arn }
        Action    = "sts:AssumeRole"
        Condition = {
          StringEquals = { "sts:ExternalId" = var.snowflake_external_id }
        }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "snowflake_bedrock" {
  role       = aws_iam_role.snowflake_bedrock.name
  policy_arn = aws_iam_policy.bedrock_invoke.arn
}
