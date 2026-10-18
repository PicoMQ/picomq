FROM debian:trixie-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends kcat \
 && rm -rf /var/lib/apt/lists/*
ENTRYPOINT ["kcat"]
