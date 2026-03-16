# ─── KMS key for at-rest encryption ───────────────────────────────────────────
resource "aws_kms_key" "kinesis" {
  description             = "KMS key for Kinesis streams encryption"
  deletion_window_in_days = 7
  enable_key_rotation     = true
  tags                    = { Name = "${var.project_name}-kinesis-kms" }
}

resource "aws_kms_alias" "kinesis" {
  name          = "alias/${var.project_name}-kinesis"
  target_key_id = aws_kms_key.kinesis.key_id
}

# ─── Kinesis Data Streams ──────────────────────────────────────────────────────
locals {
  kinesis_streams = ["cex-trades", "cex-orders", "cex-balances", "cex-logs"]
}

resource "aws_kinesis_stream" "cex" {
  for_each         = toset(local.kinesis_streams)
  name             = "${var.project_name}-${each.key}"
  shard_count      = var.kinesis_shard_count
  retention_period = var.kinesis_retention_hours

  encryption_type = "KMS"
  kms_key_id      = aws_kms_key.kinesis.id

  stream_mode_details {
    stream_mode = "PROVISIONED"
  }

  tags = { Name = "${var.project_name}-${each.key}", Stream = each.key }
}

# ─── Kinesis Firehose → S3 (for logs and batch fallback) ──────────────────────
resource "aws_kinesis_firehose_delivery_stream" "logs_to_s3" {
  name        = "${var.project_name}-logs-to-s3"
  destination = "extended_s3"

  extended_s3_configuration {
    role_arn            = aws_iam_role.firehose.arn
    bucket_arn          = aws_s3_bucket.raw.arn
    prefix              = "firehose/logs/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/hour=!{timestamp:HH}/"
    error_output_prefix = "firehose/errors/!{firehose:error-output-type}/year=!{timestamp:yyyy}/month=!{timestamp:MM}/"

    buffering_size     = 64
    buffering_interval = 60
    compression_format = "GZIP"

    cloudwatch_logging_options {
      enabled         = true
      log_group_name  = aws_cloudwatch_log_group.firehose.name
      log_stream_name = "S3Delivery"
    }
  }

  tags = { Name = "${var.project_name}-firehose-logs" }
}

resource "aws_cloudwatch_log_group" "firehose" {
  name              = "/aws/kinesisfirehose/${var.project_name}-logs"
  retention_in_days = 14
}

# ─── CloudWatch Logs subscription → Firehose ──────────────────────────────────
resource "aws_cloudwatch_log_subscription_filter" "cex_app_logs" {
  name            = "${var.project_name}-cex-app-log-filter"
  log_group_name  = "/cex/application"
  filter_pattern  = ""
  destination_arn = aws_kinesis_firehose_delivery_stream.logs_to_s3.arn
  role_arn        = aws_iam_role.cloudwatch_to_firehose.arn

  depends_on = [aws_cloudwatch_log_group.cex_app]
}

resource "aws_cloudwatch_log_group" "cex_app" {
  name              = "/cex/application"
  retention_in_days = 7
}

# ─── MSK (optional high-throughput path) ──────────────────────────────────────
resource "aws_msk_cluster" "cex" {
  count                  = var.enable_msk ? 1 : 0
  cluster_name           = "${var.project_name}-msk"
  kafka_version          = "3.6.0"
  number_of_broker_nodes = 3

  broker_node_group_info {
    instance_type   = "kafka.m5.large"
    client_subnets  = aws_subnet.private[*].id
    security_groups = [aws_security_group.msk[0].id]

    storage_info {
      ebs_storage_info {
        volume_size = 1000
        provisioned_throughput {
          enabled           = true
          volume_throughput = 250
        }
      }
    }
  }

  encryption_info {
    encryption_in_transit {
      client_broker = "TLS"
      in_cluster    = true
    }
    encryption_at_rest_kms_key_arn = aws_kms_key.kinesis.arn
  }

  client_authentication {
    sasl {
      iam = true
    }
  }

  configuration_info {
    arn      = aws_msk_configuration.cex[0].arn
    revision = aws_msk_configuration.cex[0].latest_revision
  }

  tags = { Name = "${var.project_name}-msk" }
}

resource "aws_msk_configuration" "cex" {
  count          = var.enable_msk ? 1 : 0
  name           = "${var.project_name}-msk-config"
  kafka_versions = ["3.6.0"]

  server_properties = <<-PROPS
    auto.create.topics.enable=false
    default.replication.factor=3
    min.insync.replicas=2
    num.partitions=6
    log.retention.hours=24
    message.max.bytes=10485760
  PROPS
}
