# AWS

The Terraform harness under `harness/terraform/aws` stands up a PicoMQ cluster on ECS Fargate.

## What it creates

| Piece | Role |
| --- | --- |
| VPC (optional) | Greenfield network with public and private subnets, NAT, and an optional S3 gateway endpoint. Or pass an existing VPC and at least two private subnets. |
| RDS Postgres | Shared metadata database. |
| S3 | Shared object store. Tasks use a task role, not static keys. |
| ECS Fargate | One service and task definition per node. |
| Internal ALB | Host-based routing to each node on protocol port `4437`. Health checks hit `/ready` on admin port `9090`. |
| Internal NLB | The Kafka listener. One hostname, `kafka.<domain>`, with a TCP listener port per node (`9092` for node 1, `9093` for node 2, and so on). |
| Private Route53 | Records under `domain` (default `picomq.internal`) that clients and redirects use. |
| Secrets Manager | Bootstrap token for `--auth required`. |

- `node_count = 1` uses `--routing local` and a single hostname (`domain`)
- `node_count >= 2` uses `--routing redirect` and per-node hosts `pico-N.<domain>`

Both load balancers are internal. Reach them from inside the VPC (bastion, VPN, SSM, or another workload). There is no public listener in this harness.

## Kafka

Every node runs the Kafka listener next to the HTTP one. Kafka is TCP with no Host header, so the ALB cannot route it: instead node N advertises `kafka.<domain>:<9092 + N - 1>` and the NLB forwards that port to that node. Clients bootstrap at `kafka.<domain>:9092` and follow the `Metadata` response to whichever node owns a topic.

The Kafka listener carries no authentication (see [Kafka protocol](/docs/kafka#exposure)), so the tasks run with `PICO_INSECURE_ALLOW_REMOTE=true` for that bind. HTTP and admin still require tokens. The VPC boundary is what guards Kafka, which is why nothing in this harness is public.

From inside the VPC:

```bash
kcat -b kafka.picomq.internal:9092 -L        # brokers and topics
kcat -b kafka.picomq.internal:9092 -t orders -P
```

## Prerequisites

- Terraform `>= 1.15.8`
- AWS credentials that can create the resources above (VPC/EC2, ECS, ELB, RDS, S3, IAM, Route53, Secrets Manager, CloudWatch Logs)
- An amd64 container image (default `ghcr.io/picomq/picomq:latest`)

## Configure and apply

From the repo:

```bash
cd harness/terraform/aws
cp terraform.tfvars.example terraform.tfvars
```

For a greenfield account, omit `vpc_id` and `private_subnet_ids`. Set at least a region, project name, node count, domain, image, and `db_password` (prefer alphanumeric so the Postgres URL stays simple):

```hcl
region     = "us-east-1"
project    = "picomq"
node_count = 2
domain     = "picomq.internal"
image      = "ghcr.io/picomq/picomq:latest"

db_password = "change-me"

force_destroy      = true
create_s3_endpoint = true
db_multi_az        = false
```

Leave `bootstrap_token` unset to generate one and store it in Secrets Manager. Set it only if you already have a token in the form documented under [Authentication](/docs/operations/auth).

Initialize with either local state or an S3 backend:

```bash
# Local state (fine for a quick personal bring-up. Keep terraform.tfstate until destroy)
terraform init -backend=false

# Or S3 state (recommended if you want destroy to work from another process).
# Create a dedicated bucket first, separate from the PicoMQ data bucket.
cp backend.hcl.example backend.hcl
# edit bucket / region / key

terraform init -backend-config=backend.hcl
```

If you already applied with local state and want to move it, use `terraform init -backend-config=backend.hcl -migrate-state`.

```bash
terraform plan
terraform apply
```

## After apply

```bash
terraform output
```

Useful outputs:

- `endpoints` — protocol URLs such as `http://pico-1.picomq.internal`
- `kafka_bootstrap` — `kafka.picomq.internal:9092`, and `kafka_brokers` for the per-node advertised addresses
- `bootstrap_secret_arn` — Secrets Manager ARN for the bootstrap token
- `alb_dns_name`, `kafka_nlb_dns_name`, `vpc_id`, `bucket`, `meta_endpoint` — for wiring clients and debugging

Fetch the bootstrap token (value is the secret string):

```bash
aws secretsmanager get-secret-value \
  --secret-id "$(terraform output -raw bootstrap_secret_arn)" \
  --query SecretString --output text
```

From a host that resolves the private zone and can reach the ALB:

```bash
curl -H "Authorization: Bearer <bootstrap-token>" \
  http://pico-1.picomq.internal/
```

Admin API and dashboard listen on `9090` on the tasks. They are not published on either load balancer. Reach them with a tunnel, sidecar, or security-group path into the task ENIs, same idea as keeping Fly's admin listener private.

## Existing VPC

Set both `vpc_id` and at least two `private_subnet_ids`. Those subnets need a path to the internet for image pulls (NAT or equivalent) unless you mirror the image privately. Set `create_s3_endpoint = true` if you want a gateway endpoint attached to the private route tables.

## Teardown

```bash
terraform destroy
```

With `force_destroy = true`, the S3 bucket can be deleted even when it still has objects. NAT Gateway and RDS are the main cost drivers while the stack is up.
