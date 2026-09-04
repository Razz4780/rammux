# Image for rammux-perf: both sides of the benchmark in one binary.
#
# Must be built from the workspace root, because rammux-perf depends on the rammux
# crate by path:
#
#   docker build -t rammux-perf -f ./rammux-perf/Dockerfile .

FROM rust:1.95-bookworm AS build

WORKDIR /build

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
RUN touch rammux/src/lib.rs rammux-perf/src/main.rs \
    && cargo build --release --package rammux-perf \
    && strip target/release/rammux-perf

FROM debian:bookworm-slim

COPY --from=build /build/target/release/rammux-perf /usr/local/bin/rammux-perf

RUN useradd --create-home --uid 10001 rammux
USER 10001

ENTRYPOINT ["/usr/local/bin/rammux-perf"]
