resource "aws_db_subnet_group" "meta" {
  name       = "${var.project}-meta"
  subnet_ids = local.private_subnet_ids

  tags = {
    Name = "${var.project}-meta"
  }
}

resource "aws_db_instance" "meta" {
  identifier     = "${var.project}-meta"
  engine         = "postgres"
  engine_version = "16"

  instance_class        = var.db_instance_class
  allocated_storage     = 20
  max_allocated_storage = 100
  storage_type          = "gp3"
  storage_encrypted     = true

  db_name  = "picomq"
  username = "picomq"
  password = var.db_password

  db_subnet_group_name   = aws_db_subnet_group.meta.name
  vpc_security_group_ids = [aws_security_group.rds.id]
  publicly_accessible    = false
  multi_az               = var.db_multi_az

  backup_retention_period   = 7
  deletion_protection       = !var.force_destroy
  skip_final_snapshot       = var.force_destroy
  final_snapshot_identifier = var.force_destroy ? null : "${var.project}-meta-final"

  apply_immediately = true

  tags = {
    Name = "${var.project}-meta"
  }
}
