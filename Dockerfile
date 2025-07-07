# syntax=docker/dockerfile:experimental

# Chef
FROM rust:1.84.1 AS chef
RUN cargo install cargo-chef

WORKDIR app

# Planner
FROM chef AS planner
COPY . .
RUN cargo chef prepare  --recipe-path recipe.json

# Builder
FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
## Build dependencies - this is the caching Docker layer!
RUN --mount=type=ssh cargo chef cook --release --recipe-path recipe.json
## Build application
COPY . .

ARG BUILD_COMMIT

RUN BUILD_COMMIT=${BUILD_COMMIT} BUILD_DATE=$(date +"%Y-%m-%dT%H:%M:%S%z") cargo build --release --bin myaku

# Runtime
FROM debian:bookworm-slim AS runtime

ARG USERNAME=appuser
ARG UID=1000
ARG GID=1000

RUN apt-get update
RUN apt-get install -y --no-install-recommends ca-certificates git openssh-client

RUN update-ca-certificates

RUN groupadd -g ${GID} ${USERNAME} && \
    useradd -u ${UID} -g ${GID} -m ${USERNAME}

COPY --from=builder /app/target/release/myaku /usr/local/bin

WORKDIR app

RUN chown -R ${UID}:${GID} /app

USER ${USERNAME}

ENTRYPOINT ["/usr/local/bin/myaku"]
