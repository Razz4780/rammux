# Image for rammux-perf: both sides of the benchmark and the cluster runner,
# in one binary.
#
# Built from the workspace root, because rammux-perf depends on the rammux
# crate by path:
#
#   docker build -t rammux-perf:dev .

FROM rust:1.95-bookworm AS build

WORKDIR /build

# Manifests first, and a dummy source tree, so that the dependency build is
# cached and only rebuilds when a manifest actually changes.
COPY Cargo.toml Cargo.lock ./
COPY rammux/Cargo.toml rammux/
COPY rammux-perf/Cargo.toml rammux-perf/
RUN mkdir -p rammux/src rammux-perf/src \
    && echo "fn main() {}" > rammux-perf/src/main.rs \
    && touch rammux/src/lib.rs \
    && cargo build --release --package rammux-perf \
    && rm -r rammux/src rammux-perf/src

COPY rammux/ rammux/
COPY rammux-perf/ rammux-perf/
# The dummy build left fingerprints behind that would otherwise let cargo
# believe the real sources are already built.
RUN touch rammux/src/lib.rs rammux-perf/src/main.rs \
    && cargo build --release --package rammux-perf \
    && strip target/release/rammux-perf

# Not distroless or scratch: a benchmark image is worth being able to exec
# into when a run behaves oddly, and Chaos Mesh debugging usually means
# looking at the pod from the inside.
FROM debian:bookworm-slim

# The client verifies the server's certificate against the bundle it is given,
# not against the system roots, but a k8s API client built from a service
# account does need the system roots.
RUN apt-get update \
    && apt-get install --no-install-recommends --yes ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /build/target/release/rammux-perf /usr/local/bin/rammux-perf

# Neither side needs to be root, and Chaos Mesh works on the pod's network
# namespace from outside, so nothing here needs NET_ADMIN either.
RUN useradd --create-home --uid 10001 rammux
USER 10001

ENTRYPOINT ["/usr/local/bin/rammux-perf"]
