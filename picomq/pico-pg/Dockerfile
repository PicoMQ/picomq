# syntax=docker/dockerfile:1

ARG PG_MAJOR=17

FROM node:22-bookworm-slim AS dashboard
WORKDIR /src/dashboard
COPY dashboard/package.json dashboard/package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY dashboard/ ./
RUN npm run build

FROM rust:1-bookworm AS build
ARG PG_MAJOR
RUN apt-get update \
 && apt-get install -y --no-install-recommends postgresql-common \
 && /usr/share/postgresql-common/pgdg/apt.postgresql.org.sh -y \
 && apt-get install -y --no-install-recommends \
        build-essential \
        clang \
        cmake \
        libclang-dev \
        pkg-config \
        postgresql-server-dev-${PG_MAJOR} \
 && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY rust-toolchain.toml ./
RUN --mount=type=cache,id=picomq-cargo-registry,sharing=locked,target=/usr/local/cargo/registry \
    --mount=type=cache,id=picomq-cargo-git,sharing=locked,target=/usr/local/cargo/git \
    cargo install --locked cargo-pgrx --version 0.16.1 \
 && cargo pgrx init --pg${PG_MAJOR} /usr/lib/postgresql/${PG_MAJOR}/bin/pg_config
COPY . .
COPY --from=dashboard /src/picomq/pico-http/_dashboard /src/picomq/pico-http/_dashboard
WORKDIR /src/picomq/pico-pg
RUN --mount=type=cache,id=picomq-pg-target-${PG_MAJOR},sharing=locked,target=/src/picomq/pico-pg/target \
    --mount=type=cache,id=picomq-cargo-registry,sharing=locked,target=/usr/local/cargo/registry \
    --mount=type=cache,id=picomq-cargo-git,sharing=locked,target=/usr/local/cargo/git \
    cargo pgrx package --no-default-features --features pg${PG_MAJOR} \
        --pg-config /usr/lib/postgresql/${PG_MAJOR}/bin/pg_config \
        --out-dir /package

FROM scratch AS package
ARG PG_MAJOR
COPY --from=build /package/usr/lib/postgresql/${PG_MAJOR}/lib/ /lib/
COPY --from=build /package/usr/share/postgresql/${PG_MAJOR}/extension/ /extension/

FROM postgres:${PG_MAJOR}-bookworm
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/*
COPY --from=build /package/ /
COPY picomq/pico-pg/init.sql /docker-entrypoint-initdb.d/pico.sql
RUN echo "shared_preload_libraries = 'pico'" >> /usr/share/postgresql/postgresql.conf.sample
EXPOSE 4437 9090 9092
