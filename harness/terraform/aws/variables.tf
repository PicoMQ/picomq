variable "project" {
  type    = string
  default = "picomq"
}

variable "region" {
  type    = string
  default = "us-east-1"
}

variable "node_count" {
  type    = number
  default = 1

  validation {
    condition     = var.node_count >= 1
    error_message = "node_count must be at least 1."
  }
}

variable "image" {
  type    = string
  default = "ghcr.io/picomq/picomq:latest"
}

variable "vpc_id" {
  type    = string
  default = null
}

variable "private_subnet_ids" {
  type    = list(string)
  default = []

  validation {
    condition = (
      (var.vpc_id == null && length(var.private_subnet_ids) == 0) ||
      (var.vpc_id != null && length(var.private_subnet_ids) >= 2)
    )
    error_message = "Omit private_subnet_ids when vpc_id is null. When vpc_id is set provide at least two private subnet ids."
  }
}

variable "domain" {
  type    = string
  default = "picomq.internal"
}

variable "db_instance_class" {
  type    = string
  default = "db.t4g.micro"
}

variable "db_password" {
  type      = string
  sensitive = true
}

variable "db_multi_az" {
  type    = bool
  default = true
}

variable "bootstrap_token" {
  type      = string
  default   = null
  sensitive = true
}

variable "task_cpu" {
  type    = number
  default = 512
}

variable "task_memory" {
  type    = number
  default = 1024
}

variable "force_destroy" {
  type    = bool
  default = false
}

variable "create_s3_endpoint" {
  type    = bool
  default = false
}
