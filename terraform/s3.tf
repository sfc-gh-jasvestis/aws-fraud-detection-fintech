# ─── KMS key for S3 encryption ────────────────────────────────────────────────
resource "aws_kms_key" "s3" {
  description             = "KMS key for S3 bucket encryption"
  deletion_window_in_days = 7
  enable_key_rotation     = true
  tags                    = { Name = "${var.project_name}-s3-kms" }
}

resource "aws_kms_alias" "s3" {
  name          = "alias/${var.project_name}-s3"
  target_key_id = aws_kms_key.s3.key_id
}

# ─── RAW landing bucket ────────────────────────────────────────────────────────
resource "aws_s3_bucket" "raw" {
  bucket        = "${var.project_name}-raw-${var.aws_account_id}"
  force_destroy = var.environment == "demo" ? true : false
  tags          = { Name = "${var.project_name}-raw", DataClassification = "confidential" }
}

resource "aws_s3_bucket_versioning" "raw" {
  bucket = aws_s3_bucket.raw.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "raw" {
  bucket = aws_s3_bucket.raw.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.s3.arn
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "raw" {
  bucket                  = aws_s3_bucket.raw.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "raw" {
  bucket = aws_s3_bucket.raw.id

  rule {
    id     = "transition-to-glacier"
    status = "Enabled"

    filter { prefix = "" }

    transition {
      days          = var.s3_raw_lifecycle_days
      storage_class = "GLACIER_IR"
    }

    expiration {
      days = 365
    }
  }

  rule {
    id     = "abort-multipart"
    status = "Enabled"
    filter { prefix = "" }
    abort_incomplete_multipart_upload { days_after_initiation = 7 }
  }
}

# SQS queue for Snowpipe event notifications
resource "aws_sqs_queue" "snowpipe_notifications" {
  name                      = "${var.project_name}-snowpipe-notifications"
  message_retention_seconds = 86400
  visibility_timeout_seconds = 30

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = { Service = "s3.amazonaws.com" }
        Action    = "sqs:SendMessage"
        Resource  = "arn:aws:sqs:${var.aws_region}:${var.aws_account_id}:${var.project_name}-snowpipe-notifications"
        Condition = {
          ArnLike = { "aws:SourceArn" = aws_s3_bucket.raw.arn }
        }
      }
    ]
  })

  tags = { Name = "${var.project_name}-snowpipe-sqs" }
}

resource "aws_s3_bucket_notification" "snowpipe" {
  bucket = aws_s3_bucket.raw.id

  queue {
    queue_arn     = aws_sqs_queue.snowpipe_notifications.arn
    events        = ["s3:ObjectCreated:*"]
    filter_prefix = "events/"
  }

  depends_on = [aws_sqs_queue.snowpipe_notifications]
}

# ─── Archive / DLQ bucket ─────────────────────────────────────────────────────
resource "aws_s3_bucket" "archive" {
  bucket        = "${var.project_name}-archive-${var.aws_account_id}"
  force_destroy = var.environment == "demo" ? true : false
  tags          = { Name = "${var.project_name}-archive", DataClassification = "confidential" }
}

resource "aws_s3_bucket_versioning" "archive" {
  bucket = aws_s3_bucket.archive.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "archive" {
  bucket = aws_s3_bucket.archive.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.s3.arn
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "archive" {
  bucket                  = aws_s3_bucket.archive.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "archive" {
  bucket = aws_s3_bucket.archive.id

  rule {
    id     = "deep-archive"
    status = "Enabled"
    filter { prefix = "" }
    transition {
      days          = 7
      storage_class = "DEEP_ARCHIVE"
    }
  }
}

# ─── Lambda deployment artifacts bucket ───────────────────────────────────────
resource "aws_s3_bucket" "lambda_artifacts" {
  bucket        = "${var.project_name}-lambda-artifacts-${var.aws_account_id}"
  force_destroy = true
  tags          = { Name = "${var.project_name}-lambda-artifacts" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lambda_artifacts" {
  bucket = aws_s3_bucket.lambda_artifacts.id
  rule {
    apply_server_side_encryption_by_default { sse_algorithm = "AES256" }
  }
}

resource "aws_s3_bucket_public_access_block" "lambda_artifacts" {
  bucket                  = aws_s3_bucket.lambda_artifacts.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}
