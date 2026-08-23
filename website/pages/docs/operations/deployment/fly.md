# Fly

Fly.io is a quick way to get a cluster running: nodes are zero-disk so they run as plain Machines with no volumes, Tigris provides the S3-compatible bucket from the same platform, and any Postgres works as the metadata database. The configs are in `harness/fly`, `fly.toml` for a single node and `fly.cluster.toml` for one app per node.

::: info Note
Fly is a good way to get started with little setup. At scale, a hyperscaler deployment on AWS or GCP is likely the better choice.
:::

## Single node

`harness/fly/fly.toml` runs one node with `--routing local`, publishes the protocol listener on `4437` behind Fly's HTTPS proxy, and keeps the admin listener private. From the repo root:

```bash
fly launch --no-deploy --copy-config --config harness/fly/fly.toml
fly storage create --name picomq-data
fly secrets set PICO_META_URL='postgres://user:pass@host:5432/picomq'
fly secrets set PICO_STORAGE='-2@s3://picomq-data?region=auto&endpoint=https://fly.storage.tigris.dev'
fly secrets set PICO_AUTH_BOOTSTRAP_TOKEN=...   # see Authentication
fly deploy --config harness/fly/fly.toml
```

`fly storage create` provisions the Tigris bucket and injects `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` as app secrets, which is where the node reads credentials from. The Tigris endpoint goes on the storage URI itself, since the injected `AWS_ENDPOINT_URL_S3` variable is not read by the server. For `PICO_META_URL` use Fly Postgres or any hosted Postgres, and a single node can skip Postgres entirely by using a SQLite file on a small volume instead.

Verify the deploy:

```bash
curl https://picomq.fly.dev/                 # list streams
fly proxy 9090                               # tunnel to the admin listener
```

## Cluster

Redirect routing needs every node to have its own address that clients can reach, so a multi-node cluster on Fly is one app per node, not several Machines behind one hostname. `fly.cluster.toml` is deployed once per node with the app name and the per-node values overridden:

```bash
fly deploy --config harness/fly/fly.cluster.toml -a picomq-1 \
    -e PICO_NODE_ID=1 -e PICO_HTTP_ADDRESS=https://picomq-1.fly.dev
fly deploy --config harness/fly/fly.cluster.toml -a picomq-2 \
    -e PICO_NODE_ID=2 -e PICO_HTTP_ADDRESS=https://picomq-2.fly.dev
```

Every app gets the same `PICO_META_URL`, `PICO_STORAGE`, and `PICO_AUTH_BOOTSTRAP_TOKEN` secrets, plus the Tigris credentials, which makes them one cluster. `PICO_HTTP_ADDRESS` must be the app's public URL because it is used verbatim in redirects.

## What the config pins down

A few choices in the configs matter and are worth keeping when adapting them.

`auto_stop_machines` is off. A stopped Machine is a crashed node. The cluster recovers, streams revive elsewhere or wait for the restart, but it is a recovery path, not a scaling mechanism. Node count changes should go through the admin transfer workflow instead.

The health check hits `/ready` on the admin port, so Fly routes traffic only after the node has registered. Combined with `--shutdown-drain-sec` this makes `fly deploy` a clean rolling restart.

The admin listener is not published. It is authenticated like the protocol listener, but the control plane has no business on the public internet, so it stays on the private network, reachable through `fly proxy 9090` or from other apps in the organization.

The WAL keeps its default `batchInterval` of 250ms, which is the right setting for object storage over the network.
