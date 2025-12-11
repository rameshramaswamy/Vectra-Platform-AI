# infrastructure/terraform/iam.tf

# 1. OIDC Provider (Connects EKS to IAM)
data "tls_certificate" "eks" {
  url = module.eks.cluster_oidc_issuer_url
}

resource "aws_iam_openid_connect_provider" "eks" {
  client_id_list  = ["sts.amazonaws.com"]
  thumbprint_list = [data.tls_certificate.eks.certificates[0].sha1_fingerprint]
  url             = module.eks.cluster_oidc_issuer_url
}

# 2. IAM Policy for S3 Access
resource "aws_iam_policy" "s3_access" {
  name        = "vectra_s3_policy"
  description = "Allow pod to write to data lake"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = ["s3:PutObject", "s3:GetObject", "s3:ListBucket"]
        Effect = "Allow"
        Resource = [
          aws_s3_bucket.vectra_datalake.arn,
          "${aws_s3_bucket.vectra_datalake.arn}/*"
        ]
      }
    ]
  })
}

# 3. IAM Role for the Consumer Service Account
resource "aws_iam_role" "consumer_role" {
  name = "vectra_consumer_role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRoleWithWebIdentity"
        Effect = "Allow"
        Principal = {
          Federated = aws_iam_openid_connect_provider.eks.arn
        }
        Condition = {
          StringEquals = {
            "${replace(aws_iam_openid_connect_provider.eks.url, "https://", "")}:sub" = "system:serviceaccount:default:stream-consumer-sa"
          }
        }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "consumer_s3_attach" {
  role       = aws_iam_role.consumer_role.name
  policy_arn = aws_iam_policy.s3_access.arn
}