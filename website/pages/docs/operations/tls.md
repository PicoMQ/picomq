# TLS

One certificate and key turn on TLS for every listener of a node. The flags are under [Configuration](/docs/operations/configuration#tls), and who may call once the connection is up is covered in [Authentication](/docs/operations/auth).

```bash
pico serve \
    --tls-certificate /etc/pico/certificate.pem --tls-key /etc/pico/key.pem \
    --http-address https://node1.internal:4437 --kafka-advertise node1.internal:9092 \
    --auth required --auth-bootstrap-token-file /run/secrets/pico-root \
    ...
```

| Listener | With TLS |
| --- | --- |
| Stream and admin | HTTPS. Clients negotiate HTTP/2 or HTTP/1.1 during the handshake |
| Kafka | `SASL_SSL` with auth required, `SSL` with auth off |

A listener speaks either TLS or plaintext, never both. A plaintext client gets a TLS alert and the connection closes. Without `--http-address`, a node with TLS advertises an `https://` address.

## The certificate

- **Format.** A PEM certificate chain, leaf first, and its PEM private key.
- **Names.** The certificate must cover every name clients dial: the hosts in `--http-address`, `--admin-address`, and `--kafka-advertise`, and any load balancer name that passes TLS through to the node. Redirects and Kafka metadata send clients to those names directly.
- **Clusters.** Nodes forward admin requests to each other over HTTPS at their `--admin-address`, and check the certificate like any client. A cluster can share one certificate whose names cover every node, or a wildcard, or give each node its own.
- **Key usage.** The certificate must list `serverAuth` in its extended key usage. Clients that verify through macOS reject a certificate without it. Public CAs include it, and the openssl recipe below asks for it.

## Rotation

A node rereads both files every minute. When they change, it checks the new pair before using it: the PEM must parse and the key must match the certificate.

- **A valid pair** is served to new connections at once. Connections already open keep running, since a certificate only matters during the handshake.
- **An invalid pair** is not used. The node keeps serving the current certificate and logs a warning.

Renewing a certificate therefore needs no restart.

Turning TLS on or off does need a restart, together with a matching change on every client.

## Handshakes

Each client has `--tls-handshake-timeout-sec`, 10 seconds by default, to finish its handshake. Handshakes run per connection, so a stalled or malicious one never holds up other clients.

## Clients

A certificate from a public CA works with every client as is, other nodes included. A private CA has to be trusted explicitly.

| Client | Trusting a private CA |
| --- | --- |
| `pico` CLI | `--ca-certificate` or `PICO_CA_CERTIFICATE`, a PEM file of CA certificates added to the system's |
| `picomq-client` | `ClientConfig::ca_certificate`, the same PEM bytes |
| Other nodes | `--tls-ca-certificate` or `PICO_TLS_CA_CERTIFICATE` on every node, for the admin requests they forward |
| librdkafka clients, kcat | `security.protocol=SASL_SSL` and `ssl.ca.location` |
| Java Kafka clients | `security.protocol=SASL_SSL`, `ssl.truststore.type=PEM`, and `ssl.truststore.location` |

`pico bench --protocol kafka` speaks `SASL_PLAINTEXT` only, so it cannot benchmark the Kafka listener of a node running TLS.

## Trying it locally

A throwaway CA and a certificate for `localhost` take three openssl commands:

```bash
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes -days 30 \
    -subj "/CN=Local CA" -keyout ca-key.pem -out ca.pem
openssl req -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes \
    -subj "/CN=localhost" -keyout key.pem -out request.pem
openssl x509 -req -days 30 -in request.pem -CA ca.pem -CAkey ca-key.pem -CAcreateserial \
    -out certificate.pem \
    -extfile <(printf 'subjectAltName=DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth')
```

Then serve with it and point each client at `ca.pem`:

```bash
pico serve --tls-certificate certificate.pem --tls-key key.pem --auth required ...

pico --endpoint https://localhost:4437 --ca-certificate ca.pem head /streams/orders
kcat -b localhost:9092 -L -X security.protocol=SASL_SSL -X ssl.ca.location=ca.pem \
    -X sasl.mechanisms=PLAIN -X sasl.username=dev/root -X sasl.password="$TOKEN"
```

The two-node compose cluster takes TLS as an extra layer, see [Docker](/docs/operations/deployment/docker#images-and-compose).
