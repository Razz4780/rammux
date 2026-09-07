//! Running both ends over a link we control, without asking for privileges.
//!
//! A measurement is only worth as much as the link under it, so the harness
//! builds its own: two network namespaces joined by a veth pair, with `netem`
//! shaping each end. Every knob that could otherwise explain a result away is
//! either set here or recorded - the socket buffer ceilings, the congestion
//! control, the impairment itself.
//!
//! # Why it needs no privileges
//!
//! Root of a *new user namespace* owns every namespace it goes on to create,
//! which is all the power this setup needs, so the harness re-executes itself
//! into one rather than demanding `sudo`. Two details make that work, and both
//! are easy to get wrong:
//!
//! * The re-execution also unshares the **network** namespace. `ip netns add`
//!   returns to the namespace it started in, and mapped root may only do that
//!   for one its own user namespace owns.
//! * It also unshares the **mount** namespace and puts a tmpfs on `/run`,
//!   because `ip netns` keeps its namespace files in `/run/netns` and mapped
//!   root cannot create that directory on the host's filesystem.
//!
//! Running as real root skips all of it and works the same way.
//!
//! # Why cleanup is a `Drop`
//!
//! Namespaces and veth pairs outlive the process that made them. Teardown is
//! recorded as it is set up and undone in reverse from [`Drop`], so an error
//! part-way through, or a signal, leaves the host as clean as a normal exit
//! does. In the unprivileged case the whole setup would evaporate with the user
//! namespace anyway; under real root it would not, and the same code covers
//! both.

use std::{
    ffi::OsStr,
    fs::File,
    io,
    os::unix::process::CommandExt,
    path::{Path, PathBuf},
    process::{ExitStatus, Stdio},
    time::{Duration, SystemTime},
};

use tokio::{
    fs,
    process::{self, Child},
    signal::unix::{SignalKind, signal},
    time::timeout,
};

use crate::{MeasurementArgs, args::ProtocolArgs};

/// Address given to the client end of the impaired link.
const CLIENT_IP: &str = "10.200.0.1";
/// Address given to the server end of the impaired link.
const SERVER_IP: &str = "10.200.0.2";
/// Prefix length of the subnet spanning the impaired link.
const PREFIX_LEN: u8 = 24;
/// Port the harness' server listens on.
///
/// Both peers live in their own network namespaces, so this can never clash
/// with anything running on the host.
const SERVER_PORT: u16 = 9000;
/// How long the peer that is still running gets to finish after the other one
/// has exited.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(5);
/// `min default max` for `tcp_wmem` and `tcp_rmem` in both namespaces.
///
/// The maximum is what autotuning may grow a socket buffer to. It has to stay
/// clear of the largest window under test, or the socket buffer becomes the
/// limiter and every number is a measurement of it instead of the protocol.
const SOCKET_BUFFER_SYSCTL: &str = "4096 131072 67108864";

/// The impairment applied to the link between the two namespaces.
#[derive(Debug, Clone, Copy)]
pub struct Link {
    /// Megabits per second, in each direction.
    pub bandwidth: f64,
    /// Milliseconds, in each direction.
    pub delay: u64,
    /// Fraction of packets dropped, in each direction.
    pub loss: f64,
}

/// Runs the client and the server in two network namespaces
/// joined by an impaired veth link.
///
/// Returns the exit code the process should exit with: 0 if both ends
/// succeeded, 130 if a signal cut the run short, 1 otherwise.
pub async fn run_harness(
    link: Link,
    mut output: PathBuf,
    measurement: MeasurementArgs,
    protocol: ProtocolArgs,
) -> io::Result<i32> {
    let Link {
        bandwidth,
        delay,
        loss,
    } = link;
    if !bandwidth.is_finite() || bandwidth <= 0.0 {
        return Err(io::Error::other(
            "bandwidth must be a finite number greater than 0",
        ));
    }
    if !(0.0..=1.0).contains(&loss) {
        return Err(io::Error::other("loss must be a fraction between 0 and 1"));
    }
    // Root of a new user namespace owns every namespace it goes on to create,
    // which is all the privilege the setup below needs. Nothing to do when we
    // already are root, and the re-executed harness arrives here as root too.
    if effective_uid()? != 0 {
        return Err(io::Error::other(format!(
            "failed to re-run the harness in a new user namespace: {}",
            reexec_in_user_namespace()
        )));
    }
    let exe = std::env::current_exe()?;

    output.push(format!(
        "{}",
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    ));
    fs::create_dir_all(&output).await?;
    let logs = Logs::create(&output)?;
    println!("Writing client and server logs to {}", output.display());

    // From here on the `Drop` implementation of `Network` undoes whatever was
    // set up, no matter how we leave this function.
    let mut network = Network::new();
    network.set_up(link).await?;
    println!(
        "Link ready: {bandwidth} mbit/s, {delay} ms delay, {}% loss in each direction",
        loss * 100.0
    );

    let server_addr = format!("{SERVER_IP}:{SERVER_PORT}");
    let mut server = process::Command::new("ip")
        .args(["netns", "exec", &network.server_ns])
        .arg(&exe)
        .args(["server", "--addr", &server_addr])
        .args(protocol.to_argv())
        .stdin(Stdio::null())
        .stdout(logs.server_stdout)
        .stderr(logs.server_stderr)
        .kill_on_drop(true)
        .spawn()?;
    println!("Server started");

    let mut client = process::Command::new("ip")
        .args(["netns", "exec", &network.client_ns])
        .arg(&exe)
        .args(["client", "--addr", &server_addr])
        .args(protocol.to_argv())
        .args(measurement.to_argv())
        .stdin(Stdio::null())
        .stdout(logs.client_stdout)
        .stderr(logs.client_stderr)
        .kill_on_drop(true)
        .spawn()?;
    println!("Client started");

    let mut sigint = signal(SignalKind::interrupt())?;
    let mut sigterm = signal(SignalKind::terminate())?;
    let first = tokio::select! {
        result = client.wait() => Exit::Client(result?),
        result = server.wait() => Exit::Server(result?),
        _ = sigint.recv() => Exit::Interrupted("SIGINT"),
        _ = sigterm.recv() => Exit::Interrupted("SIGTERM"),
    };

    let (client_status, server_status) = match first {
        Exit::Client(status) => {
            println!("Client exited with {status}");
            let server_status = wait_or_kill(&mut server, "server").await?;
            println!("Server exited with {server_status}");
            (Some(status), Some(server_status))
        },
        Exit::Server(status) => {
            println!("Server exited with {status}");
            let client_status = wait_or_kill(&mut client, "client").await?;
            println!("Client exited with {client_status}");
            (Some(client_status), Some(status))
        },
        Exit::Interrupted(signal) => {
            println!("Received {signal}, terminating the client and the server");
            let _ = client.start_kill();
            let _ = server.start_kill();
            client.wait().await?;
            server.wait().await?;
            (None, None)
        },
    };

    match (client_status, server_status) {
        // Interrupted by a signal, the usual shell convention.
        (None, _) | (_, None) => Ok(130),
        (Some(client), Some(server)) if client.success() && server.success() => Ok(0),
        _ => Ok(1),
    }
}

/// Which of the things we were waiting for happened first.
enum Exit {
    Client(ExitStatus),
    Server(ExitStatus),
    Interrupted(&'static str),
}

/// Waits for the peer that outlived the other one, killing it if it overstays
/// [`SHUTDOWN_GRACE`].
async fn wait_or_kill(child: &mut Child, name: &str) -> io::Result<ExitStatus> {
    if let Ok(status) = timeout(SHUTDOWN_GRACE, child.wait()).await {
        return status;
    }
    println!("The {name} is still running after {SHUTDOWN_GRACE:?}, killing it");
    let _ = child.start_kill();
    child.wait().await
}

/// The two network namespaces and the veth pair connecting them.
///
/// Whatever was created is torn down when this is dropped,
/// so an error or a signal at any point still leaves the host clean.
struct Network {
    client_ns: String,
    server_ns: String,
    client_dev: String,
    server_dev: String,
    /// Commands undoing the setup so far, in the order they were applied.
    teardown: Vec<Vec<String>>,
}

impl Network {
    /// Names are suffixed with our PID, so that concurrent harness runs
    /// do not fight over the same namespaces and interfaces.
    fn new() -> Self {
        let pid = std::process::id();
        Self {
            client_ns: format!("transit-client-{pid}"),
            server_ns: format!("transit-server-{pid}"),
            // Interface names are capped at 15 characters.
            client_dev: format!("vc{pid}"),
            server_dev: format!("vs{pid}"),
            teardown: Vec::new(),
        }
    }

    async fn set_up(&mut self, link: Link) -> io::Result<()> {
        let Link {
            bandwidth,
            delay,
            loss,
        } = link;
        for namespace in [self.client_ns.clone(), self.server_ns.clone()] {
            run(&["ip", "netns", "add", &namespace]).await?;
            self.teardown
                .push(vec!["ip".into(), "netns".into(), "del".into(), namespace]);
        }

        run(&[
            "ip",
            "link",
            "add",
            &self.client_dev,
            "type",
            "veth",
            "peer",
            "name",
            &self.server_dev,
        ])
        .await?;
        // Deleting either end takes the whole pair down, and both ends are still
        // here for now. This entry is only for failures before the moves below -
        // once both ends sit in a namespace, deleting the namespaces is enough.
        let veth_teardown = self.teardown.len();
        self.teardown.push(vec![
            "ip".into(),
            "link".into(),
            "del".into(),
            self.client_dev.clone(),
        ]);

        run(&[
            "ip",
            "link",
            "set",
            &self.client_dev,
            "netns",
            &self.client_ns,
        ])
        .await?;
        // The client end is out of reach now, so undo the pair through its peer.
        self.teardown[veth_teardown][3].clone_from(&self.server_dev);
        run(&[
            "ip",
            "link",
            "set",
            &self.server_dev,
            "netns",
            &self.server_ns,
        ])
        .await?;
        self.teardown.remove(veth_teardown);

        for (namespace, device, ip) in [
            (&self.client_ns, &self.client_dev, CLIENT_IP),
            (&self.server_ns, &self.server_dev, SERVER_IP),
        ] {
            run(&[
                "ip",
                "-n",
                namespace,
                "addr",
                "add",
                &format!("{ip}/{PREFIX_LEN}"),
                "dev",
                device,
            ])
            .await?;
            run(&["ip", "-n", namespace, "link", "set", "dev", device, "up"]).await?;
            run(&["ip", "-n", namespace, "link", "set", "dev", "lo", "up"]).await?;

            // Autotuned `SO_SNDBUF` is capped by `tcp_wmem`, and a cap below
            // the transit window binds instead of the window, quietly turning
            // every result into a measurement of the socket buffer. These are
            // per-namespace, so raising them here touches nothing on the host.
            for (knob, value) in [
                ("net.ipv4.tcp_wmem", SOCKET_BUFFER_SYSCTL),
                ("net.ipv4.tcp_rmem", SOCKET_BUFFER_SYSCTL),
            ] {
                run(&[
                    "ip",
                    "netns",
                    "exec",
                    namespace,
                    "sysctl",
                    "-q",
                    "-w",
                    &format!("{knob}={value}"),
                ])
                .await?;
            }

            // netem shapes egress only, so each end gets its own qdisc.
            // That makes every knob apply in each direction.
            let mut qdisc: Vec<String> = ["ip", "netns", "exec", namespace, "tc", "qdisc", "add"]
                .into_iter()
                .map(String::from)
                .collect();
            qdisc.extend([
                "dev".to_string(),
                device.clone(),
                "root".to_string(),
                "netem".to_string(),
                // The default queue of 1000 packets starts dropping traffic on its own
                // once the bandwidth-delay product grows, which would show up as loss
                // the caller never asked for.
                "limit".to_string(),
                "100000".to_string(),
                "delay".to_string(),
                format!("{delay}ms"),
                "rate".to_string(),
                format!("{bandwidth}mbit"),
            ]);
            if loss > 0.0 {
                qdisc.extend(["loss".to_string(), format!("{:.4}%", loss * 100.0)]);
            }
            run(&qdisc).await?;
        }

        Ok(())
    }
}

impl Drop for Network {
    fn drop(&mut self) {
        if self.teardown.is_empty() {
            return;
        }
        println!("Removing the veth pair and the network namespaces");
        // Undo in reverse, and keep going if a step fails, so that one leftover
        // does not strand the rest. Reporting is all we can do about it here.
        for command in self.teardown.drain(..).rev() {
            let status = std::process::Command::new(&command[0])
                .args(&command[1..])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
            match status {
                Ok(status) if status.success() => {},
                _ => eprintln!("Cleanup step `{}` did not succeed", command.join(" ")),
            }
        }
    }
}

/// Log files for the client and the server.
struct Logs {
    client_stdout: File,
    client_stderr: File,
    server_stdout: File,
    server_stderr: File,
}

impl Logs {
    fn create(directory: &Path) -> io::Result<Self> {
        Ok(Self {
            client_stdout: File::create(directory.join("client.stdout"))?,
            client_stderr: File::create(directory.join("client.stderr"))?,
            server_stdout: File::create(directory.join("server.stdout"))?,
            server_stderr: File::create(directory.join("server.stderr"))?,
        })
    }
}

/// Runs a setup command to completion, failing with its stderr attached.
async fn run<S: AsRef<OsStr>>(raw_command: &[S]) -> io::Result<()> {
    let mut command = process::Command::new(raw_command.first().unwrap());
    command.args(raw_command.get(1..).unwrap_or_default());
    let output = command.output().await?;
    if output.status.success() {
        return Ok(());
    }

    Err(io::Error::other(format!(
        "`{command:?}` failed with {}: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr).trim()
    )))
}

/// Replaces this process with the same harness run as root of a new user
/// namespace, so that no privileges on the host are needed.
///
/// The new namespace only grants power over what it owns, hence `--net`:
/// `ip netns add` returns to the network namespace it started in, and it may
/// only do that for one this user namespace owns.
///
/// Only ever returns the error that kept the process from being replaced.
fn reexec_in_user_namespace() -> io::Error {
    let exe = match std::env::current_exe() {
        Ok(exe) => exe,
        Err(error) => return error,
    };
    std::process::Command::new("unshare")
        .args(["--user", "--map-root-user", "--mount", "--net", "--"])
        // `ip netns` keeps its namespace files in `/run/netns`, which mapped
        // root may not create, so the harness gets a private mount namespace
        // with a fresh tmpfs there. `$@` is the harness command line below.
        .args([
            "sh",
            "-c",
            "mount -t tmpfs tmpfs /run && mkdir /run/netns && exec \"$@\"",
            "transit-harness",
        ])
        .arg(exe)
        .args(std::env::args_os().skip(1))
        .exec()
}

/// Reads the effective UID of this process out of `/proc/self/status`.
fn effective_uid() -> io::Result<u32> {
    let status = std::fs::read_to_string("/proc/self/status")?;
    status
        .lines()
        .find_map(|line| line.strip_prefix("Uid:"))
        .and_then(|uids| uids.split_whitespace().nth(1))
        .and_then(|uid| uid.parse().ok())
        .ok_or_else(|| io::Error::other("failed to read the effective UID from /proc/self/status"))
}
