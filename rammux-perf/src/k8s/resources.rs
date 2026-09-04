//! The objects a cluster run creates, and the labels that let it find them
//! again.
//!
//! Everything is built from `serde_json` and deserialized into the typed
//! object, which reads much closer to the YAML anyone debugging a run will be
//! looking at than the builder form does.

use anyhow::Context as _;
use k8s_openapi::api::{
    batch::v1::Job,
    core::v1::{ConfigMap, Pod, Secret, Service},
};
use kube::api::{DynamicObject, ObjectMeta};
use serde_json::{Value, json};

use crate::{
    config::MuxerConfig,
    k8s::{K8sConfig, archetype::LinkProfile},
};

/// Label whose value is the run id, on every object a run creates.
///
/// Teardown deletes by this and nothing else, so an interrupted run cleans up
/// exactly what it made even if it never got as far as recording it.
pub const RUN_LABEL: &str = "rammux-perf.metalbear.com/run";
/// Label separating the two sides, which the chaos selectors match on.
pub const COMPONENT_LABEL: &str = "rammux-perf.metalbear.com/component";

/// Where the config file is mounted in both pods.
const CONFIG_DIR: &str = "/etc/rammux-perf";
/// Where the TLS bundle is mounted in both pods.
const CERT_DIR: &str = "/etc/rammux-perf/tls";
/// Port the echo server's plain HTTP listener binds.
pub const HTTP_PORT: u16 = 8080;
/// Port the echo server's HTTPS listener binds.
pub const HTTPS_PORT: u16 = 8443;
/// UDP port the echo server's QUIC listener binds.
pub const QUIC_PORT: u16 = 8444;

/// Names and labels for one run's objects.
pub struct Naming {
    /// Short random id, distinguishing concurrent runs in one namespace.
    pub run_id: String,
    pub namespace: String,
}

impl Naming {
    pub fn new(namespace: String) -> Self {
        let run_id = format!("{:08x}", rand::random::<u32>());
        Self { run_id, namespace }
    }

    pub fn server_pod(&self) -> String {
        format!("rammux-perf-server-{}", self.run_id)
    }

    pub fn client_job(&self) -> String {
        format!("rammux-perf-client-{}", self.run_id)
    }

    pub fn server_config_map(&self) -> String {
        format!("rammux-perf-server-config-{}", self.run_id)
    }

    pub fn client_config_map(&self) -> String {
        format!("rammux-perf-client-config-{}", self.run_id)
    }

    pub fn secret(&self) -> String {
        format!("rammux-perf-tls-{}", self.run_id)
    }

    pub fn gate_service(&self) -> String {
        format!("rammux-perf-gate-{}", self.run_id)
    }

    pub fn netem_chaos(&self) -> String {
        format!("rammux-perf-netem-{}", self.run_id)
    }

    pub fn bandwidth_chaos(&self) -> String {
        format!("rammux-perf-bandwidth-{}", self.run_id)
    }

    /// The selector every teardown and every lookup goes through.
    pub fn run_selector(&self) -> String {
        format!("{RUN_LABEL}={}", self.run_id)
    }

    fn labels(&self, component: &str) -> Value {
        json!({
            "app.kubernetes.io/name": "rammux-perf",
            RUN_LABEL: self.run_id,
            COMPONENT_LABEL: component,
        })
    }

    fn meta(&self, name: String, component: &str) -> Value {
        json!({
            "name": name,
            "namespace": self.namespace,
            "labels": self.labels(component),
        })
    }
}

/// The TLS bundle, as the one PEM file [`crate::tls::CertBundle`] expects.
pub fn secret(naming: &Naming, cert_pem: &str) -> anyhow::Result<Secret> {
    serde_json::from_value(json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": naming.meta(naming.secret(), "shared"),
        "stringData": { "cert.pem": cert_pem },
    }))
    .context("failed to build the TLS secret")
}

/// One side's config file.
///
/// A map per side rather than one shared map, because the client's config
/// names the server's address and so cannot be written until the server pod
/// has one. Rewriting a mounted map in place would work only as fast as
/// kubelet's sync period.
pub fn config_map(name: String, naming: &Naming, config: &Value) -> anyhow::Result<ConfigMap> {
    serde_json::from_value(json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": naming.meta(name, "shared"),
        "data": { "config.json": serde_json::to_string_pretty(config)? },
    }))
    .context("failed to build a config map")
}

/// The server's config.
///
/// Carries the muxer config only for QUIC, whose windows are transport
/// parameters: the server has to have them before it binds, so unlike the
/// other three it cannot learn them from the client's upgrade request. The
/// client sends its copy anyway and the server rejects a mismatch, so writing
/// both from `muxer` here is what keeps them equal.
pub fn server_config(muxer: &MuxerConfig) -> Value {
    let mut config = json!({
        "http_addr": format!("0.0.0.0:{HTTP_PORT}"),
        "https_addr": format!("0.0.0.0:{HTTPS_PORT}"),
        "quic_addr": format!("0.0.0.0:{QUIC_PORT}"),
        "cert_path": format!("{CERT_DIR}/cert.pem"),
    });
    if let MuxerConfig::Quic(..) = muxer {
        config["quic"] = serde_json::to_value(muxer).unwrap();
    }
    config
}

/// The client's config, pointed at the server pod's own address.
///
/// Deliberately the pod IP and not a Service: a Service would put kube-proxy's
/// DNAT and its conntrack entry in the path, and this is a latency benchmark.
pub fn client_config(naming: &Naming, config: &K8sConfig, server_ip: &str) -> Value {
    // QUIC is always encrypted and has an address of its own; the other three
    // choose between the plain and the TLS port.
    let quic = matches!(config.muxer_config, MuxerConfig::Quic(..));
    let port = match (quic, config.tls) {
        (true, _) => QUIC_PORT,
        (false, true) => HTTPS_PORT,
        (false, false) => HTTP_PORT,
    };
    let mut client_config = json!({
        "server_addr": format!("{server_ip}:{port}"),
        "iterations": config.iterations,
        "bulk_streams": config.bulk_streams,
        "bulk_stream_data": config.bulk_stream_data * 1024 * 1024,
        "muxer": config.muxer_config,
        "await_endpoint": gate_endpoint(naming),
    });
    if let Some(size) = config.ping_pong_size {
        client_config["ping_pong_size"] = json!(size);
    }
    if config.tls || quic {
        client_config["cert_path"] = json!(format!("{CERT_DIR}/cert.pem"));
    }
    client_config
}

/// The start gate: a Service that exists only once the link is impaired.
///
/// The client blocks until this accepts a connection, so it holds the
/// benchmark between "the client pod is running, and can therefore be
/// impaired" and "the impairment is in place". It points at the echo server,
/// which is already listening, so creating the Service is all it takes to
/// open the gate - there is no second workload to run and wait for.
///
/// Its name does not resolve at all until it is created, which is what makes
/// the wait a wait rather than a race. Expect the gate to open a few seconds
/// after the Service appears rather than instantly: CoreDNS caches the denial
/// it has been handing out, for 5 seconds by default.
pub fn gate_service(naming: &Naming) -> anyhow::Result<Service> {
    serde_json::from_value(json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": naming.meta(naming.gate_service(), "gate"),
        "spec": {
            "selector": { COMPONENT_LABEL: "server" },
            "ports": [{ "port": HTTP_PORT, "targetPort": HTTP_PORT }],
        },
    }))
    .context("failed to build the gate service")
}

/// Where the client waits for the gate.
pub fn gate_endpoint(naming: &Naming) -> String {
    format!(
        "{}.{}.svc:{HTTP_PORT}",
        naming.gate_service(),
        naming.namespace
    )
}

/// Keeps this run's two pods off the same node.
///
/// The scheduler usually spreads two pods anyway, but "usually" is not a
/// property a measurement campaign can rest on: co-located, the client and
/// server share a node's CPU, and CPU per byte is one of the things being
/// compared. Required rather than preferred for the same reason - a preference
/// silently not honoured produces numbers that look fine and are not.
///
/// Scoped to this run's label, so concurrent runs do not compete: each run's
/// own pair must be split, but one run's server may share a node with
/// another's client.
fn anti_affinity(naming: &Naming) -> Value {
    json!({
        "podAntiAffinity": {
            "requiredDuringSchedulingIgnoredDuringExecution": [{
                "labelSelector": { "matchLabels": { RUN_LABEL: naming.run_id } },
                "topologyKey": "kubernetes.io/hostname",
            }],
        },
    })
}

/// The echo server, as a bare pod.
///
/// No Deployment, because a rescheduled server mid-run would silently change
/// what is being measured; if this pod dies the run should fail rather than
/// quietly restart against a fresh connection.
pub fn server_pod(naming: &Naming, args: &K8sConfig) -> anyhow::Result<Pod> {
    serde_json::from_value(json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": naming.meta(naming.server_pod(), "server"),
        "spec": {
            "restartPolicy": "Never",
            "affinity": anti_affinity(naming),
            "containers": [container(
                "server",
                args,
                json!(["server", "run", "--json-log", "--config-path", format!("{CONFIG_DIR}/config.json")]),
            )],
            "volumes": volumes(naming, naming.server_config_map()),
        },
    }))
    .context("failed to build the server pod")
}

/// The client, as a Job that runs exactly once.
///
/// `backoffLimit: 0` because the client already retries a failed iteration
/// itself; a second pod would only make the logs ambiguous about which
/// attempt produced which numbers.
pub fn client_job(naming: &Naming, args: &K8sConfig) -> anyhow::Result<Job> {
    serde_json::from_value(json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": naming.meta(naming.client_job(), "client"),
        "spec": {
            "backoffLimit": 0,
            "completions": 1,
            "parallelism": 1,
            "template": {
                "metadata": { "labels": naming.labels("client") },
                "spec": {
                    "restartPolicy": "Never",
                    "affinity": anti_affinity(naming),
                    "containers": [container(
                        "client",
                        args,
                        json!(["client", "run", "--json-log", "--config-path", format!("{CONFIG_DIR}/config.json")]),
                    )],
                    "volumes": volumes(naming, naming.client_config_map()),
                },
            },
        },
    }))
    .context("failed to build the client job")
}

fn container(name: &str, args: &K8sConfig, command_args: Value) -> Value {
    json!({
        "name": name,
        "image": args.image,
        "imagePullPolicy": "IfNotPresent",
        "args": command_args,
        "env": [
            // Without this the subscriber's default filter drops every event
            // and the job produces no output to collect.
            { "name": "RUST_LOG", "value": "info" },
        ],
        "volumeMounts": [
            { "name": "config", "mountPath": CONFIG_DIR, "readOnly": true },
            { "name": "tls", "mountPath": CERT_DIR, "readOnly": true },
        ],
    })
}

fn volumes(naming: &Naming, config_map: String) -> Value {
    json!([
        { "name": "config", "configMap": { "name": config_map } },
        { "name": "tls", "secret": { "secretName": naming.secret() } },
    ])
}

/// The delay, jitter and loss half of an archetype.
///
/// # Which pods, and which direction
///
/// The selector is the run label, which matches *both* the server pod and the
/// client pod, and `direction: to` puts netem on each one's egress. That is
/// what makes the emulated link symmetric: the client's requests are delayed,
/// jittered and dropped on the way out, and so are the server's echoes.
///
/// Note what this deliberately is not. Selecting one side and naming the other
/// as `target` would be the obvious spelling, but Chaos Mesh resolves its
/// selectors once at injection and cannot inject into a pod that does not
/// exist yet, so it only works if both pods are already up - and then two
/// egress rules are simpler than one rule with a target, and avoid the
/// `direction: both` behaviour of also impairing traffic *between* the
/// selected pods.
///
/// Both pods being up before injection is what the start gate is for; see
/// [`gate_service`]. `podPhaseSelectors` pins that down: a pod that is not
/// Running has no container for the chaos daemon to enter.
///
/// The delay and jitter are per leg, so each is half the archetype's round
/// trip figure - see [`LinkProfile::one_way_delay`]. Loss and rate are not
/// halved: a path that drops 1% drops 1% in each direction, and a 50 Mbit/s
/// link carries 50 Mbit/s each way.
pub fn netem_chaos(
    naming: &Naming,
    profile: &LinkProfile,
    duration: std::time::Duration,
) -> anyhow::Result<DynamicObject> {
    let mut spec = json!({
        "action": "netem",
        "mode": "all",
        "selector": both_pods(naming),
        "direction": "to",
        // A dead orchestrator must not leave the cluster impaired forever.
        "duration": format!("{}s", duration.as_secs()),
        "delay": {
            "latency": format_millis(profile.one_way_delay()),
            "jitter": format_millis(profile.one_way_jitter()),
            "correlation": profile.delay_correlation.to_string(),
        },
    });
    if profile.loss_percent > 0.0 {
        spec["loss"] = json!({
            "loss": profile.loss_percent.to_string(),
            "correlation": profile.loss_correlation.to_string(),
        });
    }
    chaos(naming, naming.netem_chaos(), spec)
}

/// The bottleneck rate half of an archetype.
///
/// A separate object because `action` takes one value: `netem` covers delay,
/// jitter and loss, and `bandwidth` is its own token bucket. Chaos Mesh
/// chains the two on the same pod. Same pod and same direction as
/// [`netem_chaos`], for the reasons given there.
pub fn bandwidth_chaos(
    naming: &Naming,
    profile: &LinkProfile,
    rate_mbit: u32,
    duration: std::time::Duration,
) -> anyhow::Result<DynamicObject> {
    // A token bucket needs a burst allowance and a queue. The burst is ~10 ms
    // of tokens, floored at an MTU so a single packet always fits; the queue
    // is one bandwidth-delay product, the textbook sizing, floored so a
    // low-BDP link still has somewhere to put a burst.
    let rate_bytes_per_sec = u64::from(rate_mbit) * 1_000_000 / 8;
    let buffer = (rate_bytes_per_sec / 100).max(1_500);
    let limit = profile.bdp_bytes().unwrap_or(0).max(64 * 1024) + buffer;
    let spec = json!({
        "action": "bandwidth",
        "mode": "all",
        "selector": both_pods(naming),
        "direction": "to",
        "duration": format!("{}s", duration.as_secs()),
        "bandwidth": {
            "rate": format!("{rate_mbit}mbit"),
            "limit": limit,
            "buffer": buffer,
        },
    });
    chaos(naming, naming.bandwidth_chaos(), spec)
}

/// Both of the run's pods, and only while they are actually running.
fn both_pods(naming: &Naming) -> Value {
    json!({
        "namespaces": [naming.namespace],
        "labelSelectors": { RUN_LABEL: naming.run_id },
        // A pod that is not Running has no container for the chaos daemon to
        // enter, so injection would fail rather than wait.
        "podPhaseSelectors": ["Running"],
    })
}

fn chaos(naming: &Naming, name: String, spec: Value) -> anyhow::Result<DynamicObject> {
    let metadata: ObjectMeta = serde_json::from_value(naming.meta(name, "chaos"))
        .context("failed to build chaos metadata")?;
    Ok(DynamicObject {
        types: Some(kube::api::TypeMeta {
            api_version: "chaos-mesh.org/v1alpha1".to_owned(),
            kind: "NetworkChaos".to_owned(),
        }),
        metadata,
        data: json!({ "spec": spec }),
    })
}

/// Chaos Mesh takes Go durations, and rounds anything below a millisecond
/// away, so sub-millisecond delays are spelled in microseconds.
fn format_millis(duration: std::time::Duration) -> String {
    if duration.as_micros().is_multiple_of(1_000) {
        format!("{}ms", duration.as_millis())
    } else {
        format!("{}us", duration.as_micros())
    }
}
