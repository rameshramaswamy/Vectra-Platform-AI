# infrastructure/terraform/main.tf

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.23"
    }
  }
  required_version = ">= 1.3.0"
}

provider "aws" {
  region = var.aws_region
  
  default_tags {
    tags = {
      Project     = "Vectra"
      Environment = var.environment
      ManagedBy   = "Terraform"
    }
  }
}

# --- VPC Configuration ---
# Enterprise Grade: Separate VPC for isolation
module "vpc" {
  source = "terraform-aws-modules/vpc/aws"
  version = "5.1.2"

  name = "vectra-vpc-${var.environment}"
  cidr = "10.0.0.0/16"

  azs             = ["${var.aws_region}a", "${var.aws_region}b", "${var.aws_region}c"]
  private_subnets = ["10.0.1.0/24", "10.0.2.0/24", "10.0.3.0/24"]
  public_subnets  = ["10.0.101.0/24", "10.0.102.0/24", "10.0.103.0/24"]
  
  # DB Subnets for RDS PostGIS
  database_subnets = ["10.0.21.0/24", "10.0.22.0/24"]
  create_database_subnet_group = true

  # NAT Gateway required for Private Subnets (EKS Nodes) to reach Internet
  enable_nat_gateway = true
  single_nat_gateway = true # Cost saving for non-prod, set false for HA
  enable_dns_hostnames = true

  # Tags required for EKS to discover subnets
  public_subnet_tags = {
    "kubernetes.io/role/elb" = 1
  }
  private_subnet_tags = {
    "kubernetes.io/role/internal-elb" = 1
  }
}