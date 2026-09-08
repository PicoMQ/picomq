# GCP

The Terraform harness under `harness/terraform/gcp` stands up a private PicoMQ
cluster on GKE.

## What it creates

| Piece | Role |
| --- | --- |
| VPC and private subnets (optional) | Creates two regional subnets, Cloud NAT, and Private Google Access, or uses an existing VPC and two private subnet self-links. |
| GKE | Runs one PicoMQ workload per logical node on a fixed worker pool. |
| Cloud SQL PostgreSQL | Shared private metadata database through Private Service Access. |
| Cloud Storage | Shared object store accessed through Workload Identity, without service-account keys. |
| Internal GKE Ingress | Private host-based routing to each PicoMQ workload on port `4437`. |
| Private Cloud DNS | Records under `domain` for clients and redirects. |
| Secret Manager | Stores the bootstrap token for `--auth required`. |

- `node_count = 1` uses `--routing local` and the bare `domain` hostname.
- `node_count >= 2` uses `--routing redirect` and `pico-N.<domain>` hostnames.

## Prerequisites

- Terraform `>= 1.15.8`
- A GCP project with billing enabled
- Application Default Credentials, for example `gcloud auth application-default login`
- Permission to enable APIs and create GKE, Compute Engine, Cloud SQL, Cloud Storage,
  Secret Manager, Service Networking, DNS, and IAM resources
- An amd64 container image, such as `ghcr.io/picomq/picomq:latest`

The harness enables the required APIs in `harness/terraform/gcp/apis.tf`.

## Configure and apply

From the repository root:

```bash
cd harness/terraform/gcp
cp terraform.tfvars.example terraform.tfvars
```

Set `project`, `region`, `node_count`, `domain`, `image`, and `db_password` in
`terraform.tfvars`. Leave `vpc_id` and `private_subnet_ids` unset to create the
network. To use an existing network, set `vpc_id` to its network self-link and
provide at least two private subnet self-links in the selected region.

Leave `bootstrap_token` unset to generate one in Secret Manager, or provide an
existing token in the format documented under [Authentication](/docs/operations/auth).

```bash
terraform init -backend=false
terraform plan
terraform apply
```

For shared state, configure the GCS backend using `backend.hcl.example` and run
`terraform init -backend-config=backend.hcl`. Protect the state because Terraform
state contains the database password and generated bootstrap token.

## Outputs

```bash
terraform output endpoints
terraform output bucket
terraform output meta_endpoint
terraform output bootstrap_secret
```

The endpoints, DNS zone, Cloud SQL address, and load balancer are private. Access
them from a connected VPC, VPN, bastion, or another workload in the network.

PicoMQ uses `-2@gcs://<bucket>`. The runtime uses Google Application Default
Credentials, and GKE Workload Identity maps the PicoMQ Kubernetes service account
to the Google service account granted access to the bucket. No JSON key is created
or mounted on PicoMQ nodes.

## Existing VPC

Set both `vpc_id` and at least two `private_subnet_ids`. The subnets must be in the
configured region and provide private egress through Cloud NAT, or an equivalent
existing egress path, for image pulls and Google APIs.

## Teardown

```bash
terraform destroy
```

Set `force_destroy = true` only when deleting the bucket, Secret Manager secret,
GKE cluster, and Cloud SQL instance without retention protection is intended.