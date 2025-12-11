resource "aws_db_instance" "vectra_spatial_db" {
  identifier           = "vectra-spatial-db"
  engine               = "postgres"
  engine_version       = "15.4"
  instance_class       = "db.t3.large"
  allocated_storage    = 100
  db_name              = "vectra_core"
  username             = var.db_username
  password             = var.db_password
  publicly_accessible  = false
  skip_final_snapshot  = true
}

# Ensure PostGIS extension is enabled via Init Script or manual setup