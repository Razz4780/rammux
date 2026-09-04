//! Server and client, together, over loopback: one echo server, every
//! protocol against it, with and without TLS.
//!
//! Runs the built binary as two processes, the way the tool is used.

use std::{
    io::Write,
    net::{TcpListener, TcpStream},
    path::Path,
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

use tempfile::TempDir;

const BIN: &str = env!("CARGO_BIN_EXE_rammux-perf");
/// One port pair per test: the tests run in parallel, and two servers cannot
/// bind the same address.
/// Each triple is (http, https, quic); QUIC is UDP, so it may reuse a number.
const PROTOCOL_ADDRS: Addrs = ("127.0.0.1:28080", "127.0.0.1:28443", "127.0.0.1:28444");
const GATE_ADDRS: Addrs = ("127.0.0.1:28090", "127.0.0.1:28453", "127.0.0.1:28454");

type Addrs = (&'static str, &'static str, &'static str);

const RAMMUX: &str = r#"{ "protocol": "rammux",
    "stream_recv_window": 262144, "global_recv_window": 4194304,
    "transit_window": 262144, "transit_window_max": 4194304,
    "probe_interval": 20, "ping_interval": 5 }"#;
const YAMUX: &str = r#"{ "protocol": "yamux", "global_recv_window": 1073741824 }"#;
const H2: &str = r#"{ "protocol": "h2", "adaptive_window": true, "global_recv_window": 1048576, "stream_recv_window": 262144 }"#;
/// The same budget as H2, so what is compared is the protocol, not the sizing.
const QUIC: &str = r#"{ "protocol": "quic", "global_recv_window": 1048576,
    "stream_recv_window": 262144, "max_streams": 128 }"#;

/// The echo server, killed on drop.
struct Server(Child);

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn write(dir: &Path, name: &str, contents: &str) -> String {
    let path = dir.join(name);
    std::fs::File::create(&path)
        .unwrap()
        .write_all(contents.as_bytes())
        .unwrap();
    path.display().to_string()
}

fn start_server(dir: &Path, (http_addr, https_addr, quic_addr): Addrs) -> (Server, String) {
    let cert = Command::new(BIN).arg("generate-cert").output().unwrap();
    assert!(cert.status.success(), "generate-cert failed");
    let cert_path = write(dir, "cert.pem", &String::from_utf8(cert.stdout).unwrap());
    let config = write(
        dir,
        "server.json",
        &format!(
            r#"{{ "http_addr": "{http_addr}", "https_addr": "{https_addr}",
                "quic_addr": "{quic_addr}", "cert_path": "{cert_path}", "quic": {QUIC} }}"#
        ),
    );
    let child = Command::new(BIN)
        .args(["server", "run", "--config-path", &config])
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();
    let server = Server(child);

    // The server logs nothing when it is up, so knock on the door.
    let deadline = Instant::now() + Duration::from_secs(10);
    while TcpStream::connect(https_addr).is_err() {
        assert!(Instant::now() < deadline, "the server never came up");
        std::thread::sleep(Duration::from_millis(50));
    }
    (server, cert_path)
}

/// Runs the client with `muxer` and returns the per-iteration reports.
/// Every event the client logged, by message.
struct ClientLog {
    iterations: Vec<serde_json::Value>,
    /// The `Finished all iterations` line: the run's whole result.
    report: serde_json::Value,
}

fn run_client(dir: &Path, name: &str, muxer: &str, cert_path: Option<&str>) -> ClientLog {
    // QUIC is always encrypted and has its own address; the other three
    // choose between the plain and the TLS one.
    let (addr, tls) = match (muxer == QUIC, cert_path) {
        (true, Some(path)) => (PROTOCOL_ADDRS.2, format!(r#""cert_path": "{path}","#)),
        (true, None) => panic!("a QUIC run needs the certificate"),
        (false, Some(path)) => (PROTOCOL_ADDRS.1, format!(r#""cert_path": "{path}","#)),
        (false, None) => (PROTOCOL_ADDRS.0, String::new()),
    };
    let config = write(
        dir,
        &format!("{name}.json"),
        &format!(
            r#"{{ "server_addr": "{addr}", {tls} "iterations": 2, "bulk_streams": 3,
                "bulk_stream_data": 2097152, "ping_pong_size": 512, "muxer": {muxer} }}"#
        ),
    );
    let output = Command::new(BIN)
        .args(["--json-log", "client", "run", "--config-path", &config])
        .env("RUST_LOG", "info")
        .output()
        .unwrap();
    let logs = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "{name}: client failed:\n{logs}");
    assert!(
        !logs.contains("retrying"),
        "{name}: an iteration had to be retried:\n{logs}"
    );

    let of = |message: &str| -> Vec<serde_json::Value> {
        logs.lines()
            .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
            .filter(|entry| entry["fields"]["message"] == message)
            .map(|entry| entry["fields"].clone())
            .collect()
    };
    ClientLog {
        iterations: of("Iteration finished"),
        report: of("Finished all iterations")
            .pop()
            .unwrap_or_else(|| panic!("{name}: the client logged no final report:\n{logs}")),
    }
}

#[test]
fn every_protocol_against_one_server() {
    let dir = TempDir::new().unwrap();
    let (_server, cert_path) = start_server(dir.path(), PROTOCOL_ADDRS);

    for (protocol, muxer) in [
        ("rammux", RAMMUX),
        ("yamux", YAMUX),
        ("h2", H2),
        ("quic", QUIC),
    ] {
        // QUIC has no plaintext variant to run.
        for tls in if muxer == QUIC {
            [true].as_slice()
        } else {
            [false, true].as_slice()
        } {
            let name = format!("{protocol}{}", if *tls { "_tls" } else { "" });
            let log = run_client(dir.path(), &name, muxer, tls.then_some(cert_path.as_str()));

            assert_eq!(
                log.iterations.len(),
                2,
                "{name}: expected one line per iteration"
            );
            for (i, iteration) in log.iterations.iter().enumerate() {
                assert_eq!(iteration["iteration"], i + 1, "{name}");
            }

            // The final line is the whole result of a cluster run: it is what
            // `k8s::RunReport` deserializes out of the job's logs, and every
            // number the campaign compares comes from it. So every field it
            // names has to be there, and be a number.
            let report = &log.report;
            for field in [
                "iterations",
                "failures",
                "mean_bulk_elapsed_micros",
                "mean_ping_pong_latency_micros",
                "p50_ping_pong_latency_micros",
                "p99_ping_pong_latency_micros",
                "completed_ping_pongs",
                "total_time_micros",
                "total_cpu_time_micros",
            ] {
                assert!(
                    report[field].is_u64(),
                    "{name}: {field} is missing or not a number: {report}",
                );
            }
            assert_eq!(report["iterations"], 2, "{name}");
            assert_eq!(report["failures"], 0, "{name}");
            assert!(
                report["mean_bulk_elapsed_micros"].as_u64().unwrap() > 0,
                "{name}: the bulk streams measured no time: {report}",
            );
            // CPU time is not checked against zero: it comes from
            // `/proc/self/stat` in 10 ms ticks, and an iteration this small
            // can genuinely land on none of them.
            assert!(
                report["total_time_micros"].as_u64().unwrap() > 0,
                "{name}: the run measured no time at all: {report}",
            );

            let completed = report["completed_ping_pongs"].as_u64().unwrap();
            let p50 = report["p50_ping_pong_latency_micros"].as_u64().unwrap();
            let p99 = report["p99_ping_pong_latency_micros"].as_u64().unwrap();
            assert!(completed > 0, "{name}: no ping pong exchange completed");
            assert!(p50 > 0 && p50 <= p99, "{name}: p50 {p50} vs p99 {p99}");
        }
    }
}

/// The start gate holds the client until an endpoint accepts a connection.
///
/// This is what a cluster run relies on to impair the link between the client
/// pod starting and the client connecting, so it is worth knowing the client
/// really does wait rather than racing ahead. Locally the gate is a listener
/// this test opens late; in a cluster it is a Service the orchestrator creates
/// once Chaos Mesh reports the impairment injected.
#[test]
fn the_start_gate_holds_the_client() {
    let dir = TempDir::new().unwrap();
    let (_server, _cert_path) = start_server(dir.path(), GATE_ADDRS);

    // Claim a port, then drop the listener, so the address is free but
    // (almost certainly) nothing is on it yet.
    let gate = TcpListener::bind("127.0.0.1:0").unwrap();
    let gate_addr = gate.local_addr().unwrap();
    drop(gate);

    let config = write(
        dir.path(),
        "gated.json",
        &format!(
            r#"{{ "server_addr": "{}", "iterations": 1, "bulk_streams": 1,
                "bulk_stream_data": 1048576, "await_endpoint": "{gate_addr}",
                "muxer": {RAMMUX} }}"#,
            GATE_ADDRS.0,
        ),
    );
    let mut child = Command::new(BIN)
        .args(["--json-log", "client", "run", "--config-path", &config])
        .env("RUST_LOG", "info")
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();

    // Long enough that an ungated client would be finished: the whole
    // workload is one 1 MiB stream over loopback.
    std::thread::sleep(Duration::from_millis(750));
    assert!(
        child.try_wait().unwrap().is_none(),
        "the client ran to completion without waiting for the gate",
    );

    let _gate = TcpListener::bind(gate_addr).unwrap();
    let output = child.wait_with_output().unwrap();
    let logs = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "client failed after the gate:\n{logs}"
    );
    assert!(
        logs.contains("Start gate opened"),
        "the client never reported passing the gate:\n{logs}"
    );
    let reports = logs
        .lines()
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .filter(|entry| entry["fields"]["message"] == "Iteration finished")
        .count();
    assert_eq!(reports, 1, "the gated client did not run its iteration");
}
