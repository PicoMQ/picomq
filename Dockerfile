# syntax=docker/dockerfile:1
# PicoMQ node image: the `pico` binary (serve + CLI).
# Protocol / meta / storage are runtime config (flags or PICO_* env).

# Console assets, embedded into the binary by the build stage.
FROM node:24-bookworm-slim AS console
WORKDIR /src/console
COPY console/package.json console/package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY console/ ./
RUN npm run build

FROM rust:1-bookworm AS build
WORKDIR /src

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
        build-essential \
        cmake \
        libsqlite3-dev \
        pkg-config \
 && rm -rf /var/lib/apt/lists/*

COPY . .
COPY --from=console /src/picomq/pico-http/_console /src/picomq/pico-http/_console
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
