# syntax=docker/dockerfile:1
# PicoMQ node image: the `pico` binary (serve + CLI).
# Protocol / meta / storage are runtime config (flags or PICO_* env).

# Dashboard assets, embedded into the binary by the build stage.
FROM node:22-bookworm-slim AS dashboard
WORKDIR /src/dashboard
COPY dashboard/package.json dashboard/package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY dashboard/ ./
RUN npm run build

FROM rust:1-bookworm AS build
WORKDIR /src

RUN apt-get update \
 && apt-get install -y --no-install-recommends libsqlite3-dev pkg-config \
 && rm -rf /var/lib/apt/lists/*

COPY . .
COPY --from=dashboard /src/picomq/pico-http/_dashboard /src/picomq/pico-http/_dashboard
# target/ and cargo caches live on the BuildKit host so rebuilds stay incremental.
RUN --mount=type=cache,id=picomq-target,sharing=locked,target=/src/target \
    --mount=type=cache,id=picomq-cargo-registry,sharing=locked,target=/usr/local/cargo/registry \
    --mount=type=cache,id=picomq-cargo-git,sharing=locked,target=/usr/local/cargo/git \
    cargo build --locked --release -p picomq-cli \
 && cp /src/target/release/pico /src/pico

FROM debian:bookworm-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates libsqlite3-0 \
 && rm -rf /var/lib/apt/lists/*

COPY --from=build /src/pico /usr/local/bin/pico

EXPOSE 4437 9090 9092
ENTRYPOINT ["pico"]
CMD ["serve"]
