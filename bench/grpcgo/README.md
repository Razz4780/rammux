# gRPC-Go benchmark harness

Runs the same three workloads as `examples/mux_compare.rs` (bulk, echo, jobs)
and emits the same CSV events, so one summariser reads both.

Both endpoints live in one process over loopback TCP, matching the Rust
harness's `--transport tcp`. To put a Go contender on the *same emulated link*
as the Rust ones, route it through `examples/emu_proxy.rs`:

```sh
cargo build --release --example emu_proxy
go build -o ../../target/grpcbench .

# proxy owns the link; both harnesses dial it instead of each other
target/release/examples/emu_proxy --listen 127.0.0.1:9101 \
    --upstream 127.0.0.1:9001 --rate-mbit 100 --rtt-ms 40 &
target/grpcbench --workload echo --streams 8 --duration-s 22 \
    --server-port 9001 --dial 127.0.0.1:9101
```

`--init-window-kb` / `--init-conn-window-kb` pin static HTTP/2 windows; leaving
them at 0 keeps gRPC's BDP-estimating dynamic windows, which is the default and
the interesting arm.
