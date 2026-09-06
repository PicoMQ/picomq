FROM ghcr.io/picomq/picomq-connectors:latest
ADD https://github.com/picomq/picomq/releases/download/v0.1.1/libpicomq_connector_iceberg_sink-linux-amd64.tar.gz /tmp/
RUN tar -xzf /tmp/libpicomq_connector_iceberg_sink-linux-amd64.tar.gz -C /usr/local/lib && rm /tmp/*.tar.gz
