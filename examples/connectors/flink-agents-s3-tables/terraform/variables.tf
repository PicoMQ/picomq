variable "region" {
  type    = string
  default = "us-east-1"
}

variable "project" {
  type    = string
  default = "picomq-agents"
}

variable "harness_state_bucket" {
  type    = string
  default = "picomq-terraform-state"
}

variable "harness_state_key" {
  type    = string
  default = "harness/aws/terraform.tfstate"
}

variable "prefix" {
  type    = string
  default = "/examples/agents/ai-sdk/"
}

variable "namespace" {
  type    = string
  default = "agents"
}

variable "jar_path" {
  type    = string
  default = "../flink-job/target/flink-agents-s3-tables.jar"
}

variable "force_destroy" {
  type    = bool
  default = true
}
