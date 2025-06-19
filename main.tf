provider "aws" {
  region = var.aws_region
}

# S3 Buckets
resource "aws_s3_bucket" "source" {
  bucket        = var.source_bucket
  force_destroy = true
}

resource "aws_s3_bucket" "destination" {
  bucket        = var.destination_bucket
  force_destroy = true
}

resource "aws_s3_bucket" "code" {
  bucket        = var.code_bucket
  force_destroy = true
}

# Upload all .py files from scripts/ to S3 code bucket
resource "aws_s3_object" "pyspark_script" {
  for_each     = fileset("${path.module}/scripts", "*.py")
  bucket       = aws_s3_bucket.code.bucket
  key          = "scripts/${each.value}"
  source       = "${path.module}/scripts/${each.value}"
  content_type = "text/x-python"
  etag         = filemd5("${path.module}/scripts/${each.value}")
}

# IAM Role and Policy for Glue
resource "aws_iam_role" "glue_role" {
  name               = "glue_exec_role"
  assume_role_policy = file("${path.module}/policies/glue_assume_role.json")
}

resource "aws_iam_role_policy" "glue_policy" {
  name   = "glue_exec_policy"
  role   = aws_iam_role.glue_role.id
  policy = file("${path.module}/policies/glue_policy.json")
}

# Glue Catalog Database
resource "aws_glue_catalog_database" "main" {
  name = var.glue_database
}

# Glue Crawler
resource "aws_glue_crawler" "crawler" {
  name          = var.crawler_name
  role          = aws_iam_role.glue_role.arn
  database_name = aws_glue_catalog_database.main.name

  s3_target {
    path = "s3://${aws_s3_bucket.source.bucket}/raw/"
  }

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }

  depends_on = [
    aws_glue_catalog_database.main,
    aws_s3_bucket.source,
    aws_iam_role.glue_role
  ]
}

# Glue ETL Job
resource "aws_glue_job" "etl_job" {
  name     = var.job_name
  role_arn = aws_iam_role.glue_role.arn

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.code.bucket}/scripts/etl_script.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"      = "python"
    "--enable-metrics"    = "true"
    "--TempDir"           = "s3://${aws_s3_bucket.destination.bucket}/tmp/"
    "--source_path"       = "s3://${aws_s3_bucket.source.bucket}/raw/"
    "--destination_path"  = "s3://${aws_s3_bucket.destination.bucket}/curated/"
    "--extra-py-files"    = join(",", [
      "s3://${aws_s3_bucket.code.bucket}/scripts/reader.py",
      "s3://${aws_s3_bucket.code.bucket}/scripts/cleaner.py",
      "s3://${aws_s3_bucket.code.bucket}/scripts/transformer.py",
      "s3://${aws_s3_bucket.code.bucket}/scripts/writer.py"
    ])
  }

  glue_version      = "4.0"
  number_of_workers = 2
  worker_type       = "G.1X"

  depends_on = [
    aws_glue_crawler.crawler
  ]
}