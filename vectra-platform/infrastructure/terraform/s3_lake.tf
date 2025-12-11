resource "aws_s3_bucket" "vectra_datalake" {
  bucket = "vectra-raw-telemetry-${var.environment}"
}

resource "aws_s3_bucket_lifecycle_configuration" "lake_lifecycle" {
  bucket = aws_s3_bucket.vectra_datalake.id
  rule {
    id = "archive-old-traces"
    status = "Enabled"
    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
  }
}