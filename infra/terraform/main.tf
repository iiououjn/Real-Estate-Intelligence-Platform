# Dubai RE platform: S3 lake + Kinesis + Lambda consumer + Redshift Serverless.
# NOT applied in CI. Run `terraform plan` first and read it: Redshift Serverless and Kinesis cost money.
terraform {
  required_version = ">= 1.5"
  required_providers { aws = { source = "hashicorp/aws", version = "~> 5.0" } }
}
provider "aws" { region = var.region }

variable "region"          { default = "eu-west-1" }
variable "project"         { default = "dre" }
variable "bucket_name"     { description = "Globally unique S3 bucket name" }
variable "redshift_admin_password" { sensitive = true }
variable "redshift_base_rpu" { default = 8 }   # smallest allowed; keeps cost low

data "aws_caller_identity" "me" {}

# ---------------------------------------------------------------- data lake
resource "aws_s3_bucket" "lake" { bucket = var.bucket_name }
resource "aws_s3_bucket_public_access_block" "lake" {
  bucket                  = aws_s3_bucket.lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}
resource "aws_s3_bucket_server_side_encryption_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule { apply_server_side_encryption_by_default { sse_algorithm = "AES256" } }
}
resource "aws_s3_bucket_lifecycle_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    id     = "expire-raw-events"
    status = "Enabled"
    filter { prefix = "dubai-re/events/" }
    expiration { days = 90 }
  }
}

# ---------------------------------------------------------------- streaming
resource "aws_kinesis_stream" "events" {
  name             = "${var.project}-property-events"
  retention_period = 24
  stream_mode_details { stream_mode = "ON_DEMAND" }
}

data "aws_iam_policy_document" "lambda_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}
resource "aws_iam_role" "consumer" {
  name               = "${var.project}-event-consumer"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}
data "aws_iam_policy_document" "consumer" {
  statement {
    actions   = ["kinesis:GetRecords", "kinesis:GetShardIterator", "kinesis:DescribeStream", "kinesis:DescribeStreamSummary",
                 "kinesis:ListShards", "kinesis:ListStreams", "kinesis:SubscribeToShard"]
    resources = [aws_kinesis_stream.events.arn]
  }
  statement {
    actions   = ["s3:PutObject"]
    resources = ["${aws_s3_bucket.lake.arn}/dubai-re/events/*", "${aws_s3_bucket.lake.arn}/dubai-re/events_rejected/*"]
  }
  statement {
    actions   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["arn:aws:logs:*:*:*"]
  }
}
resource "aws_iam_role_policy" "consumer" {
  role   = aws_iam_role.consumer.id
  policy = data.aws_iam_policy_document.consumer.json
}

resource "aws_lambda_function" "consumer" {
  function_name    = "${var.project}-event-consumer"
  role             = aws_iam_role.consumer.arn
  runtime          = "python3.12"
  handler          = "lambda_function.handler"
  filename         = "${path.module}/lambda.zip"            # build with `make lambda-zip`
  source_code_hash = filebase64sha256("${path.module}/lambda.zip")
  timeout          = 60
  memory_size      = 256
  environment { variables = { EVENTS_BUCKET = aws_s3_bucket.lake.bucket, EVENTS_PREFIX = "dubai-re" } }
}
resource "aws_lambda_event_source_mapping" "kinesis" {
  event_source_arn                   = aws_kinesis_stream.events.arn
  function_name                      = aws_lambda_function.consumer.arn
  starting_position                  = "LATEST"
  batch_size                         = 200
  maximum_batching_window_in_seconds = 30
  function_response_types            = ["ReportBatchItemFailures"]
  maximum_retry_attempts             = 3
  bisect_batch_on_function_error     = true
}

# ---------------------------------------------------------------- warehouse
data "aws_iam_policy_document" "redshift_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["redshift.amazonaws.com"]
    }
  }
}
resource "aws_iam_role" "redshift_s3" {
  name               = "${var.project}-redshift-s3"
  assume_role_policy = data.aws_iam_policy_document.redshift_assume.json
}
resource "aws_iam_role_policy" "redshift_s3" {
  role = aws_iam_role.redshift_s3.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{ Effect = "Allow", Action = ["s3:GetObject", "s3:ListBucket"],
                   Resource = [aws_s3_bucket.lake.arn, "${aws_s3_bucket.lake.arn}/*"] }]
  })
}
resource "aws_redshiftserverless_namespace" "ns" {
  namespace_name      = "${var.project}-ns"
  db_name             = "dev"
  admin_username      = "admin"
  admin_user_password = var.redshift_admin_password
  iam_roles           = [aws_iam_role.redshift_s3.arn]
}
resource "aws_redshiftserverless_workgroup" "wg" {
  namespace_name = aws_redshiftserverless_namespace.ns.namespace_name
  workgroup_name = "${var.project}-wg"
  base_capacity  = var.redshift_base_rpu
}

output "bucket"              { value = aws_s3_bucket.lake.bucket }
output "kinesis_stream"      { value = aws_kinesis_stream.events.name }
output "redshift_role_arn"   { value = aws_iam_role.redshift_s3.arn }
output "redshift_endpoint"   { value = aws_redshiftserverless_workgroup.wg.endpoint }
