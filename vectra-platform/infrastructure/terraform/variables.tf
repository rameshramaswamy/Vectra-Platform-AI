# infrastructure/terraform/variables.tf

variable "aws_region" {
  description = "AWS Region to deploy to"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Deployment environment (dev, stage, prod)"
  type        = string
  default     = "dev"
}

variable "db_username" {
  description = "PostgreSQL username"
  type        = string
  default     = "vectra_admin"
}

variable "db_password" {
  description = "PostgreSQL password"
  type        = string
  sensitive   = true
}