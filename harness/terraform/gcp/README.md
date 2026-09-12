# PicoMQ GCP Terraform harness

This harness creates a private GKE deployment of PicoMQ with Cloud SQL PostgreSQL, a
private Cloud Storage bucket, Secret Manager, private Cloud DNS, and an internal GKE
HTTP load balancer.

## Prerequisites

- Terraform 1.15.8 or later
- A GCP project with billing enabled
- Google credentials with permission to create GKE, Compute Engine networking,
  Cloud SQL, Cloud Storage, Secret Manager, Service Networking, and IAM resources
- `gcloud auth application-default login`, or another Application Default
  Credentials configuration

The harness enables the required APIs in `apis.tf`. The deploying identity must be
allowed to enable services.

## Configure and apply

Copy `terraform.tfvars.example` to `terraform.tfvars` and set `project`, `region`,
`db_password`, and the desired `node_count`. Leave `vpc_id` and
`private_subnet_ids` unset to create a VPC and two regional private subnets. To use
an existing network, set `vpc_id` to its self-link and provide two subnet self-links
in the same region.

```text
terraform -chdir=harness/terraform/gcp init
terraform -chdir=harness/terraform/gcp plan
terraform -chdir=harness/terraform/gcp apply
```

The deployment creates one fixed GKE worker node. `node_count` controls the number
of PicoMQ workloads, not the GKE worker count. Each workload has a stable service
and hostname. A single workload uses `--routing local` and the bare domain; two or
more workloads use `--routing redirect` and `pico-1.<domain>`, `pico-2.<domain>`,
and so on.

## Outputs

```text
terraform -chdir=harness/terraform/gcp output endpoints
terraform -chdir=harness/terraform/gcp output bucket
terraform -chdir=harness/terraform/gcp output meta_endpoint
terraform -chdir=harness/terraform/gcp output bootstrap_secret_arn
```

The endpoints and DNS names are private and resolve only from networks attached to
the private Cloud DNS zone. The bootstrap token is stored in Secret Manager and
projected into the workloads through a Kubernetes Secret. Terraform state therefore
contains the token, as it does for the AWS harness; protect the state backend.

## Storage and identity

PicoMQ uses `-2@gcs://<bucket>`. The GCS backend is native `object_store` support
and uses Application Default Credentials. GKE Workload Identity maps the PicoMQ
Kubernetes service account to a Google service account with bucket object access;
no service-account JSON key is created or mounted.

## Destroy

```text
terraform -chdir=harness/terraform/gcp destroy
```

Set `force_destroy = true` only when deleting the bucket, Secret Manager secret,
and Cloud SQL instance without retention protection is intended.
