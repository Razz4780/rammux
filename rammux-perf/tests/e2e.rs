//! Server and client, together, over loopback: one echo server, every
//! protocol against it, with and without TLS.
//!
//! Runs the built binary as two processes, the way the tool is used.

use std::{
    io::Write,
    net::TcpStream,
    path::Path,
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

use tempfile::TempDir;

const BIN: &str = env!("CARGO_BIN_EXE_rammux-perf");
const HTTP_ADDR: &str = "127.0.0.1:28080";
const HTTPS_ADDR: &str = "127.0.0.1:28443";

const RAMMUX: &str = r#"{ "protocol": "rammux",
    "stream_recv_window": 262144, "global_recv_window": 4194304,
    "transit_window": 262144, "transit_window_max": 4194304,
    "probe_interval": 20, "ping_interval": 5 }"#;
const YAMUX: &str = r#"{ "protocol": "yamux", "global_recv_window": 1073741824 }"#;
const H2: &str = r#"{ "protocol": "h2", "adaptive_window": true, "global_recv_window": 1048576, "stream_recv_window": 262144 }"#;

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

fn start_server(dir: &Path) -> (Server, String) {
    let cert = Command::new(BIN).arg("generate-cert").output().unwrap();
    assert!(cert.status.success(), "generate-cert failed");
    let cert_path = write(dir, "cert.pem", &String::from_utf8(cert.stdout).unwrap());
    let config = write(
        dir,
        "server.json",
        &format!(
            r#"{{ "http_addr": "{HTTP_ADDR}", "https_addr": "{HTTPS_ADDR}", "cert_path": "{cert_path}" }}"#
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
    while TcpStream::connect(HTTPS_ADDR).is_err() {
        assert!(Instant::now() < deadline, "the server never came up");
        std::thread::sleep(Duration::from_millis(50));
    }
    (server, cert_path)
}

/// Runs the client with `muxer` and returns the per-iteration reports.
fn run_client(
    dir: &Path,
    name: &str,
    muxer: &str,
    cert_path: Option<&str>,
) -> Vec<serde_json::Value> {
    let (addr, tls) = match cert_path {
        Some(path) => (HTTPS_ADDR, format!(r#""cert_path": "{path}","#)),
        None => (HTTP_ADDR, String::new()),
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
        .args(["client", "run", "--json-log", "--config-path", &config])
        .env("RUST_LOG", "info")
        .output()
        .unwrap();
    let logs = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "{name}: client failed:\n{logs}");
    assert!(
        !logs.contains("retrying"),
        "{name}: an iteration had to be retried:\n{logs}"
    );

    logs.lines()
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .filter(|entry| entry["fields"]["message"] == "Iteration finished")
        .map(|entry| entry["fields"].clone())
        .collect()
}

#[test]
fn every_protocol_against_one_server() {
    let dir = TempDir::new().unwrap();
    let (_server, cert_path) = start_server(dir.path());

    for (protocol, muxer) in [("rammux", RAMMUX), ("yamux", YAMUX), ("h2", H2)] {
        for tls in [false, true] {
            let name = format!("{protocol}{}", if tls { "_tls" } else { "" });
            let reports = run_client(dir.path(), &name, muxer, tls.then_some(cert_path.as_str()));
            assert_eq!(
                reports.len(),
                2,
                "{name}: expected one report per iteration"
            );
            for (i, report) in reports.iter().enumerate() {
                assert_eq!(report["iteration"], i + 1, "{name}");
                assert_eq!(report["attempt"], 1, "{name}");
                assert_eq!(report["bulk_streams"], 3, "{name}");
                let mbps = report["bulk_mbps_p50"].as_f64().unwrap();
                assert!(mbps > 0.0, "{name}: no throughput measured");
                let exchanges = report["ping_pong_count"].as_u64().unwrap();
                assert!(exchanges > 0, "{name}: no ping pong exchange completed");
                let latency = report["latency_ms_p50"].as_f64().unwrap();
                assert!(latency > 0.0, "{name}: no latency measured");
                assert!(report["cpu_ms"].is_u64(), "{name}: cpu_ms is not a number");
                assert!(
                    report["elapsed_ms"].is_u64(),
                    "{name}: elapsed_ms is not a number"
                );
            }
        }
    }
}
