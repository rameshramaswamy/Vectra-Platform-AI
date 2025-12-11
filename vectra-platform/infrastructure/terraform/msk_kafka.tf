# infrastructure/terraform/msk_kafka.tf

resource "aws_security_group" "msk_sg" {
  name_prefix = "vectra-msk-sg-"
  vpc_id      = module.vpc.vpc_id

  # Allow Inbound traffic from EKS Nodes (Private Subnets)
  ingress {
    from_port   = 9092
    to_port     = 9092
    protocol    = "tcp"
    cidr_blocks = module.vpc.private_subnets_cidr_blocks
  }

  # Allow TLS traffic
  ingress {
    from_port   = 9094
    to_port     = 9094
    protocol    = "tcp"
    cidr_blocks = module.vpc.private_subnets_cidr_blocks
  }
}

resource "aws_msk_cluster" "vectra_kafka" {
  cluster_name           = "vectra-kafka-${var.environment}"
  kafka_version          = "3.4.0"
  number_of_broker_nodes = 2 # Minimum for HA

  broker_node_group_info {
    instance_type = "kafka.t3.small" # Cost-effective for Dev
    client_subnets = slice(module.vpc.private_subnets, 0, 2)
    security_groups = [aws_security_group.msk_sg.id]
    
    storage_info {
      ebs_storage_info {
        volume_size = 50
      }
    }
  }

  encryption_info {
    encryption_in_transit {
      client_broker = "PLAINTEXT" # Simplified for internal VPC traffic
      in_cluster    = true
    }
  }

  configuration_info {
    arn      = aws_msk_configuration.vectra_kafka_config.arn
    revision = aws_msk_configuration.vectra_kafka_config.latest_revision
  }
}

resource "aws_msk_configuration" "vectra_kafka_config" {
  kafka_versions = ["3.4.0"]
  name           = "vectra-kafka-config-${var.environment}"

  server_properties = <<PROPERTIES
auto.create.topics.enable = true
delete.topic.enable = true
PROPERTIES
}

# Output the Bootstrap Brokers to use in our App Config
output "kafka_bootstrap_brokers" {
  description = "Comma separated list of one or more hostname:port pairs"
  value       = aws_msk_cluster.vectra_kafka.bootstrap_brokers
}