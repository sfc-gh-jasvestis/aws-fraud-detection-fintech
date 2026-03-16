# ─── SSM Parameters for Lambda config ────────────────────────────────────────
resource "aws_ssm_parameter" "kinesis_stream_trades" {
  name  = "/${var.project_name}/kinesis/stream/cex-trades"
  type  = "String"
  value = aws_kinesis_stream.cex["cex-trades"].name
}

resource "aws_ssm_parameter" "kinesis_stream_onchain" {
  name  = "/${var.project_name}/kinesis/stream/cex-logs"
  type  = "String"
  value = aws_kinesis_stream.cex["cex-logs"].name
}

# ─── Lambda: On-Chain Indexer ─────────────────────────────────────────────────
# Triggered by EventBridge schedule; calls 3rd-party blockchain API and
# normalizes on-chain events into Kinesis or S3.

data "archive_file" "onchain_indexer" {
  type        = "zip"
  source_dir  = "${path.module}/../ingestion/lambda/onchain_indexer"
  output_path = "${path.module}/.build/onchain_indexer.zip"
}

resource "aws_s3_object" "onchain_indexer" {
  bucket = aws_s3_bucket.lambda_artifacts.id
  key    = "lambda/onchain_indexer.zip"
  source = data.archive_file.onchain_indexer.output_path
  etag   = data.archive_file.onchain_indexer.output_md5
}

resource "aws_lambda_function" "onchain_indexer" {
  function_name = "${var.project_name}-onchain-indexer"
  role          = aws_iam_role.lambda_exec.arn
  handler       = "handler.lambda_handler"
  runtime       = var.lambda_runtime
  timeout       = 300
  memory_size   = 512

  s3_bucket        = aws_s3_bucket.lambda_artifacts.id
  s3_key           = aws_s3_object.onchain_indexer.key
  source_code_hash = data.archive_file.onchain_indexer.output_base64sha256

  vpc_config {
    subnet_ids         = aws_subnet.private[*].id
    security_group_ids = [aws_security_group.lambda.id]
  }

  environment {
    variables = {
      KINESIS_STREAM_NAME = aws_kinesis_stream.cex["cex-logs"].name
      S3_BUCKET           = aws_s3_bucket.raw.bucket
      S3_PREFIX           = "onchain-events/"
      ENVIRONMENT         = var.environment
      LOG_LEVEL           = "INFO"
    }
  }

  tracing_config { mode = "Active" }

  tags = { Name = "${var.project_name}-onchain-indexer", Function = "onchain-indexer" }
}

resource "aws_cloudwatch_log_group" "onchain_indexer" {
  name              = "/aws/lambda/${var.project_name}-onchain-indexer"
  retention_in_days = 14
}

resource "aws_cloudwatch_event_rule" "onchain_indexer" {
  name                = "${var.project_name}-onchain-indexer-schedule"
  description         = "Poll on-chain indexer every 30 seconds"
  schedule_expression = "rate(1 minute)"
}

resource "aws_cloudwatch_event_target" "onchain_indexer" {
  rule      = aws_cloudwatch_event_rule.onchain_indexer.name
  target_id = "onchain-indexer"
  arn       = aws_lambda_function.onchain_indexer.arn
}

resource "aws_lambda_permission" "onchain_indexer_events" {
  statement_id  = "AllowEventBridgeInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.onchain_indexer.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.onchain_indexer.arn
}

# ─── Lambda: Log Normalizer ───────────────────────────────────────────────────
# Triggered by Kinesis Firehose transformation; parses raw CEX logs and
# normalizes to structured JSON before S3 landing.

data "archive_file" "log_normalizer" {
  type        = "zip"
  source_dir  = "${path.module}/../ingestion/lambda/log_normalizer"
  output_path = "${path.module}/.build/log_normalizer.zip"
}

resource "aws_s3_object" "log_normalizer" {
  bucket = aws_s3_bucket.lambda_artifacts.id
  key    = "lambda/log_normalizer.zip"
  source = data.archive_file.log_normalizer.output_path
  etag   = data.archive_file.log_normalizer.output_md5
}

resource "aws_lambda_function" "log_normalizer" {
  function_name = "${var.project_name}-log-normalizer"
  role          = aws_iam_role.lambda_exec.arn
  handler       = "handler.lambda_handler"
  runtime       = var.lambda_runtime
  timeout       = 60
  memory_size   = 256

  s3_bucket        = aws_s3_bucket.lambda_artifacts.id
  s3_key           = aws_s3_object.log_normalizer.key
  source_code_hash = data.archive_file.log_normalizer.output_base64sha256

  environment {
    variables = {
      ENVIRONMENT = var.environment
      LOG_LEVEL   = "INFO"
    }
  }

  tracing_config { mode = "Active" }

  tags = { Name = "${var.project_name}-log-normalizer", Function = "log-normalizer" }
}

resource "aws_cloudwatch_log_group" "log_normalizer" {
  name              = "/aws/lambda/${var.project_name}-log-normalizer"
  retention_in_days = 14
}

# ─── Lambda Dead Letter Queue ─────────────────────────────────────────────────
resource "aws_sqs_queue" "lambda_dlq" {
  name                      = "${var.project_name}-lambda-dlq"
  message_retention_seconds = 1209600 # 14 days
  tags                      = { Name = "${var.project_name}-lambda-dlq" }
}
