# AWS Lambda function
resource "aws_lambda_function" "aws_lambda_sst_one_explode" {
  image_uri     = "${data.aws_ecr_repository.podaac_sst_repo_zero.repository_url}:latest"
  function_name = "${var.prefix}-sst-zero"
  role          = data.aws_iam_role.lambda_execution_role.arn
  package_type  = "Image"
  memory_size   = 6144
  timeout       = 900
}

resource "aws_lambda_function" "aws_lambda_sst_one_explode" {
  image_uri     = "${data.aws_ecr_repository.podaac_sst_repo_one.repository_url}:latest"
  function_name = "${var.prefix}-sst-one"
  role          = data.aws_iam_role.lambda_execution_role.arn
  package_type  = "Image"
  memory_size   = 6144
  timeout       = 900
}

resource "aws_lambda_function" "aws_lambda_sst_two_write" {
  image_uri     = "${data.aws_ecr_repository.podaac_sst_repo_two.repository_url}:latest"
  function_name = "${var.prefix}-sst-two"
  role          = data.aws_iam_role.lambda_execution_role.arn
  package_type  = "Image"
  memory_size   = 6144
  timeout       = 900
}

resource "aws_lambda_function" "aws_lambda_sst_three_stats" {
  image_uri     = "${data.aws_ecr_repository.podaac_sst_repo_three.repository_url}:latest"
  function_name = "${var.prefix}-sst-three"
  role          = data.aws_iam_role.lambda_execution_role.arn
  package_type  = "Image"
  memory_size   = 6144
  timeout       = 900
}

resource "aws_lambda_function" "aws_lambda_sst_four_grid" {
  image_uri     = "${data.aws_ecr_repository.podaac_sst_repo_four.repository_url}:latest"
  function_name = "${var.prefix}-sst-four"
  role          = data.aws_iam_role.lambda_execution_role.arn
  package_type  = "Image"
  memory_size   = 6144
  timeout       = 900
}

resource "aws_lambda_permission" "allow_lambda" {
  statement_id  = "AllowExecutionFromLambda"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.aws_lambda_sst_two_write.function_name
  principal     = "s3.amazonaws.com"
  source_arn = aws_lambda_function.aws_lambda_sst_one_explode.arn
}

# SSM Parameter Store EDL Credentials
resource "aws_ssm_parameter" "aws_ssm_parameter_edl_username" {
  name        = "${var.prefix}-sst-edl-username"
  description = "Earthdata Login username"
  type        = "SecureString"
  value       = var.edl_username
}

resource "aws_ssm_parameter" "aws_ssm_parameter_edl_password" {
  name        = "${var.prefix}-sst-edl-password"
  description = "Earthdata Login password"
  type        = "SecureString"
  value       = var.edl_password
}

# S3 Bucket to hold results
resource "aws_s3_bucket" "aws_s3_bucket_sst" {
  bucket = "${var.prefix}-sst"
  tags   = { Name = "${var.prefix}-sst" }
}

resource "aws_s3_bucket_public_access_block" "aws_s3_bucket_sst_public_block" {
  bucket                  = aws_s3_bucket.aws_s3_bucket_sst.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "aws_s3_bucket_sst_ownership" {
  bucket = aws_s3_bucket.aws_s3_bucket_sst.id
  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "aws_s3_bucket_sst_encryption" {
  bucket = aws_s3_bucket.aws_s3_bucket_sst.bucket
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}