# infrastructure/terraform/eks_cluster.tf

module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "~> 19.0"

  cluster_name    = "vectra-cluster-${var.environment}"
  cluster_version = "1.27"

  # Network integration
  vpc_id                         = module.vpc.vpc_id
  subnet_ids                     = module.vpc.private_subnets
  cluster_endpoint_public_access = true

  # OIDC Provider for IAM Roles for Service Accounts (IRSA)
  enable_irsa = true

  # --- Node Groups (The Workers) ---
  eks_managed_node_groups = {
    general_purpose = {
      min_size     = 2
      max_size     = 5
      desired_size = 2

      instance_types = ["t3.large"]
      capacity_type  = "ON_DEMAND"
      
      # Ensure nodes have access to talk to other AWS services if needed
      iam_role_additional_policies = {
        AmazonSSMManagedInstanceCore = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
      }
    }
  }

  # Security Group Rules
  node_security_group_additional_rules = {
    ingress_self_all = {
      description = "Node to node all ports/protocols"
      protocol    = "-1"
      from_port   = 0
      to_port     = 0
      type        = "ingress"
      self        = true
    }
  }
}

# Provider config to interact with the cluster immediately after creation
provider "kubernetes" {
  host                   = module.eks.cluster_endpoint
  cluster_ca_certificate = base64decode(module.eks.cluster_certificate_authority_data)
  exec {
    api_version = "client.authentication.k8s.io/v1beta1"
    command     = "aws"
    args        = ["eks", "get-token", "--cluster-name", module.eks.cluster_name]
  }
}