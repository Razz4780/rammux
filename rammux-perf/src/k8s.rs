//! Runs one benchmark on a Kubernetes cluster, over a Chaos Mesh emulated link.
//!
//! The shape of a run is: put the echo server on the cluster, start the client
//! as a job and hold it at a start gate, impair the path between the two, open
//! the gate, read what the client measured out of the job's logs, and remove
//! everything again.
//!
//! The order is what it is because of one Chaos Mesh property: it resolves its
//! selectors once, at injection, and cannot inject into a pod that does not
//! exist yet. So both pods have to be running before the impairment goes in -
//! which is why the client starts first - and the client must not connect
//! until it has, which is what the gate is for. The result is that both ends
//! are impaired, and the client's very first packet already crosses the
//! emulated link.
//!
//! Every object a run creates carries the run's id as a label, and teardown
//! deletes by that label alone. Nothing is remembered in memory that teardown
//! depends on, so a run interrupted anywhere - including between building an
//! object and hearing back that it was created - still cleans up after itself.
//!
//! # What the cluster has to provide
//!
//! * Chaos Mesh, for any archetype but `none`. The run fails with a message
//!   saying so if the `NetworkChaos` kind is not served.
//! * A kernel with `sch_netem`, which is what Chaos Mesh's chaos daemon puts
//!   the delay, jitter and loss in. Most distribution kernels have it as a
//!   module; a stripped one will make the injection fail rather than quietly
//!   run the benchmark unimpaired.
//! * Two schedulable nodes. The run's two pods are required to land on
//!   different ones, so that neither is measuring the other's CPU; on a
//!   single-node cluster the client pod stays Pending and the run says so.
//! * An image with this binary as its entrypoint that the nodes can pull.
//! * Credentials that may create and delete pods, jobs, services, config maps
//!   and secrets in the namespace, `networkchaos` in `chaos-mesh.org`, and - only
//!   if the namespace does not exist yet - namespaces.

use std::{path::Path, time::Duration};

use anyhow::Context as _;
use k8s_openapi::api::{
    batch::v1::Job,
    core::v1::{ConfigMap, Namespace, Pod, Secret, Service},
};
use kube::{
    Api, Client,
    api::{
        DeleteParams, DynamicObject, ListParams, LogParams, ObjectMeta, PostParams,
        PropagationPolicy,
    },
    discovery,
};
use schemars::JsonSchema;
use serde::Deserialize;
use tokio::time::Instant;

use crate::{
    config::MuxerConfig,
    k8s::{
        archetype::Archetype,
        resources::{COMPONENT_LABEL, Naming},
    },
    signal::ExitSignal,
    tls,
};

pub mod archetype;
mod resources;

/// How often the cluster is asked whether it has caught up yet.
const POLL_INTERVAL: Duration = Duration::from_secs(1);
/// How long the server pod and the chaos injection each get to become ready.
const SETUP_TIMEOUT: Duration = Duration::from_secs(180);

#[derive(Deserialize, JsonSchema)]
pub struct K8sConfig {
    /// Container image with the `rammux-perf` binary as its entrypoint.
    pub image: String,

    /// Which emulated link to run over.
    pub archetype: Archetype,

    /// Muxer configuration.
    pub muxer_config: MuxerConfig,

    /// Number of iterations to run.
    pub iterations: usize,

    /// How many bulk streams each iteration runs.
    pub bulk_streams: usize,

    /// How much data each bulk stream sends, and reads back, in MiB.
    pub bulk_stream_data: usize,

    /// Size of the ping pong message in bytes. Omitted runs no ping pong
    /// stream, and so measures no latency.
    #[serde(default)]
    pub ping_pong_size: Option<usize>,

    /// Encrypt the benchmarked connection with TLS 1.3.
    #[serde(default)]
    pub tls: bool,

    /// How long the client job gets before the run is given up on.
    #[serde(default = "K8sConfig::default_timeout_s")]
    pub timeout_secs: u64,
}

impl K8sConfig {
    fn default_timeout_s() -> u64 {
        900
    }

    fn timeout(&self) -> Duration {
        Duration::from_secs(self.timeout_secs)
    }
}

pub async fn run(config: &Path) -> anyhow::Result<()> {
    let config = {
        let raw = std::fs::read(config)
            .with_context(|| format!("failed to read config at {}", config.display()))?;
        serde_json::from_slice::<K8sConfig>(&raw)
            .with_context(|| format!("failed to parse config at {}", config.display()))?
    };

    let naming = Naming::new(format!("rammux-perf-{:08x}", rand::random::<u32>()));
    let profile = config.archetype.profile();

    let client = {
        let config = kube::Config::infer().await?;
        kube::Client::try_from(config).map_err(anyhow::Error::new)?
    };

    tracing::info!(
        run_id = naming.run_id,
        namespace = naming.namespace,
        archetype = config.archetype.name(),
        protocol = config.muxer_config.protocol(),
        rtt_ms = profile.map(|p| p.rtt.as_millis() as u64),
        jitter_ms = profile.map(|p| p.jitter.as_millis() as u64),
        loss_percent = profile.map(|p| p.loss_percent),
        rate_mbit = profile.and_then(|p| p.rate_mbit),
        link_bdp_bytes = profile.and_then(|p| p.bdp_bytes()),
        "Starting a cluster run",
    );

    let mut signal = ExitSignal::new()?;
    let outcome = tokio::select! {
        () = &mut signal => {
            tracing::warn!("Received an exit signal, tearing down");
            Err(anyhow::anyhow!("interrupted"))
        },
        result = execute(&client, &config, &naming) => result,
    };

    Api::<Namespace>::all(client)
        .delete(
            &naming.namespace,
            &DeleteParams {
                propagation_policy: Some(PropagationPolicy::Background),
                ..Default::default()
            },
        )
        .await
        .inspect_err(|error| {
            tracing::error!(
                error = format!("{error:#}"),
                namespace = naming.namespace,
                "Failed to delete the bench namespace, objects may be left behind",
            );
        })?;

    let report = outcome?;
    println!("FAILURES: {}", report.failures);
    println!("MEAN BULK ELAPSED: {}ms", report.mean_bulk_elapsed_ms);
    println!(
        "MEAN PING PONG LATENCY: {}ms",
        report.mean_ping_pong_latency_ms
    );
    println!(
        "P50 PING PONG LATENCY: {}ms",
        report.p50_ping_pong_latency_ms
    );
    println!(
        "P99 PING PONG LATENCY: {}ms",
        report.p99_ping_pong_latency_ms
    );
    println!("COMPLETED PING PONG: {}", report.completed_ping_pongs);

    Ok(())
}

#[derive(Deserialize)]
struct RunReport {
    failures: u64,
    mean_bulk_elapsed_ms: u64,
    mean_ping_pong_latency_ms: u64,
    p50_ping_pong_latency_ms: u64,
    p99_ping_pong_latency_ms: u64,
    completed_ping_pongs: u64,
}

impl RunReport {
    fn from_logs(logs: &str) -> Option<Self> {
        logs.lines()
            .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
            .find_map(|object| {
                let serde_json::Value::Object(mut object) = object else {
                    return None;
                };
                let fields = object.remove("fields")?;
                if fields.get("message")?.as_str()? != "Finished all iterations" {
                    return None;
                };
                serde_json::from_value(fields)
                    .inspect_err(|error| {
                        tracing::warn!(%error, "Failed to deserialize run report");
                    })
                    .ok()
            })
    }
}

/// Creates everything, waits for it, and reads the result out of the job.
async fn execute(
    client: &Client,
    config: &K8sConfig,
    naming: &Naming,
) -> anyhow::Result<RunReport> {
    let ns = &naming.namespace;
    tracing::info!(
        namespace = ns,
        "Creating resources in an ephemeral namespace",
    );

    Api::<Namespace>::all(client.clone())
        .create(
            &PostParams::default(),
            &Namespace {
                metadata: ObjectMeta {
                    name: Some(naming.namespace.clone()),
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .await
        .inspect(|_| tracing::info!("Created bench namespace"))
        .context("failed to create the namespace")?;

    let bundle = tls::generate_cert()?;
    let cert_pem = format!(
        "{}{}",
        bundle.cert.pem(),
        bundle.signing_key.serialize_pem()
    );
    Api::<Secret>::namespaced(client.clone(), ns)
        .create(
            &PostParams::default(),
            &resources::secret(naming, &cert_pem)?,
        )
        .await
        .inspect(|_| tracing::info!("Created shared TLS secret"))
        .context("failed to create the TLS secret")?;

    let config_maps = Api::<ConfigMap>::namespaced(client.clone(), ns);
    config_maps
        .create(
            &PostParams::default(),
            &resources::config_map(
                naming.server_config_map(),
                naming,
                &resources::server_config(&config.muxer_config),
            )?,
        )
        .await
        .inspect(|_| tracing::info!("Created server config map"))
        .context("failed to create the server config map")?;

    let pods = Api::<Pod>::namespaced(client.clone(), ns);
    pods.create(
        &PostParams::default(),
        &resources::server_pod(naming, config)?,
    )
    .await
    .inspect(|_| tracing::info!("Created server pod"))
    .context("failed to create the server pod")?;
    let server_ip = wait_for("the server pod to be ready", SETUP_TIMEOUT, || async {
        let status = pods
            .get(&naming.server_pod())
            .await?
            .status
            .unwrap_or_default();
        if let Some(reason) = stuck_reason(&status) {
            anyhow::bail!("the server pod cannot start: {reason}");
        }
        let ready = status
            .container_statuses
            .unwrap_or_default()
            .iter()
            .all(|container| container.ready);
        Ok(ready.then_some(status.pod_ip).flatten())
    })
    .await?;
    tracing::info!(server_ip, "Server is ready");

    // The client's config names the server's address, so it can only be
    // written now.
    config_maps
        .create(
            &PostParams::default(),
            &resources::config_map(
                naming.client_config_map(),
                naming,
                &resources::client_config(naming, config, &server_ip),
            )?,
        )
        .await
        .inspect(|_| tracing::info!("Created client config map"))
        .context("failed to create the client config map")?;

    // The client starts before the impairment and waits at the start gate for
    // it. Chaos Mesh needs a running container to inject into, and the client
    // must not connect until it has - the gate is what makes those two
    // compatible, and it is why the client is created here rather than last.
    let jobs = Api::<Job>::namespaced(client.clone(), ns);
    jobs.create(
        &PostParams::default(),
        &resources::client_job(naming, config)?,
    )
    .await
    .inspect(|_| tracing::info!("Created client job"))
    .context("failed to create the client job")?;
    wait_for("the client pod to be running", SETUP_TIMEOUT, || async {
        let Some(pod) = client_pod(client, naming).await? else {
            return Ok(None);
        };
        let status = pod.status.unwrap_or_default();
        if let Some(reason) = stuck_reason(&status) {
            anyhow::bail!("the client pod cannot start: {reason}");
        }
        Ok((status.phase.as_deref() == Some("Running")).then_some(()))
    })
    .await?;
    tracing::info!("Client is running, held at the start gate");

    if let Some(profile) = config.archetype.profile() {
        let (api_resource, _) = discovery::pinned_kind(
            client,
            &kube::core::GroupVersionKind::gvk("chaos-mesh.org", "v1alpha1", "NetworkChaos"),
        )
        .await
        .context(
            "failed to find the NetworkChaos resource - is Chaos Mesh installed on this cluster?",
        )?;
        let chaos: Api<DynamicObject> = Api::namespaced_with(client.clone(), ns, &api_resource);

        // The chaos outlives the job by a margin, so a slow teardown cannot
        // unimpair the link while the client is still measuring it.
        let chaos_duration = config.timeout() + Duration::from_secs(120);
        let mut objects = vec![resources::netem_chaos(naming, &profile, chaos_duration)?];
        if let Some(rate_mbit) = profile.rate_mbit {
            objects.push(resources::bandwidth_chaos(
                naming,
                &profile,
                rate_mbit,
                chaos_duration,
            )?);
        }
        for object in &objects {
            chaos
                .create(&PostParams::default(), object)
                .await
                .inspect(|_| tracing::info!("Created NetworkChaos experiment"))
                .context("failed to create a NetworkChaos experiment")?;
        }
        for object in &objects {
            let name = object.metadata.name.clone().expect("named above");
            wait_for(&format!("{name} to be injected"), SETUP_TIMEOUT, || async {
                let injected = chaos.get(&name).await?;
                Ok(all_injected(&injected).then_some(()))
            })
            .await?;
        }
        tracing::info!(
            archetype = config.archetype.name(),
            experiments = objects.len(),
            one_way_delay = ?profile.one_way_delay(),
            one_way_jitter = ?profile.one_way_jitter(),
            "Link impairment is in place, on both pods",
        );
    }

    // Opening the gate is the last thing to happen, so everything the client
    // does - its very first SYN included - crosses the emulated link.
    Api::<Service>::namespaced(client.clone(), ns)
        .create(&PostParams::default(), &resources::gate_service(naming)?)
        .await
        .context("failed to open the start gate")?;
    tracing::info!(
        endpoint = resources::gate_endpoint(naming),
        "Start gate open, benchmark running",
    );

    wait_for("the client job to finish", config.timeout(), || async {
        let status = jobs
            .get(&naming.client_job())
            .await?
            .status
            .unwrap_or_default();
        if status.succeeded.unwrap_or(0) > 0 {
            return Ok(Some(()));
        }
        if status.failed.unwrap_or(0) > 0 {
            anyhow::bail!("the client pod failed");
        }
        // A job whose pod cannot start never succeeds and never fails; it
        // would only ever hit the timeout, which says nothing useful.
        if let Some(pod) = client_pod(client, naming).await?
            && let Some(reason) = stuck_reason(&pod.status.unwrap_or_default())
        {
            anyhow::bail!("the client pod cannot start: {reason}");
        }
        Ok(None)
    })
    .await?;

    let client_logs = client_logs(client, naming).await?;
    for line in client_logs.lines() {
        tracing::debug!(line, "Got client log");
    }
    let report = RunReport::from_logs(&client_logs)
        .context("final run report was not found in the client logs")?;

    Ok(report)
}

/// Whether Chaos Mesh reports every selected pod as impaired.
fn all_injected(object: &DynamicObject) -> bool {
    object.data["status"]["conditions"]
        .as_array()
        .into_iter()
        .flatten()
        .any(|condition| condition["type"] == "AllInjected" && condition["status"] == "True")
}

/// The pod the client job produced, once it has produced one.
async fn client_pod(client: &Client, naming: &Naming) -> anyhow::Result<Option<Pod>> {
    let listed = Api::<Pod>::namespaced(client.clone(), &naming.namespace)
        .list(&ListParams::default().labels(&format!(
            "{},{COMPONENT_LABEL}=client",
            naming.run_selector()
        )))
        .await
        .context("failed to look for the client pod")?;
    Ok(listed.items.into_iter().next())
}

/// Everything the client job wrote.
async fn client_logs(client: &Client, naming: &Naming) -> anyhow::Result<String> {
    let pod = client_pod(client, naming)
        .await?
        .context("the client job produced no pod")?;
    let name = pod
        .metadata
        .name
        .as_deref()
        .context("client pod has no name")?;
    Api::<Pod>::namespaced(client.clone(), &naming.namespace)
        .logs(name, &LogParams::default())
        .await
        .context("failed to read the client job's logs")
}

/// Why a pod will never start, if that is where it has ended up.
///
/// A pod waiting on an image it cannot pull, or a volume it cannot mount, sits
/// in `Pending` indefinitely: nothing about it ever becomes ready and nothing
/// ever fails, so without this a wrong `--image` costs the full timeout and
/// reports only that the wait expired.
fn stuck_reason(status: &k8s_openapi::api::core::v1::PodStatus) -> Option<String> {
    const TERMINAL: &[&str] = &[
        "ErrImagePull",
        "ImagePullBackOff",
        "InvalidImageName",
        "CreateContainerConfigError",
        "CreateContainerError",
        "CrashLoopBackOff",
    ];
    if status.phase.as_deref() == Some("Failed") {
        return Some(status.reason.clone().unwrap_or_else(|| "failed".to_owned()));
    }
    // A pod no node will take has no containers, and so no container status to
    // read a reason out of - it sits in Pending until the wait gives up. The
    // scheduler's own message says why, and with anti-affinity required, "no
    // second node" is a likely why.
    if let Some(unschedulable) = status.conditions.iter().flatten().find(|condition| {
        condition.type_ == "PodScheduled"
            && condition.status == "False"
            && condition.reason.as_deref() == Some("Unschedulable")
    }) {
        return Some(match &unschedulable.message {
            Some(message) => format!("unschedulable: {message}"),
            None => "unschedulable".to_owned(),
        });
    }
    status
        .container_statuses
        .iter()
        .flatten()
        .chain(status.init_container_statuses.iter().flatten())
        .find_map(|container| {
            let waiting = container.state.as_ref()?.waiting.as_ref()?;
            let reason = waiting.reason.as_deref()?;
            TERMINAL.contains(&reason).then(|| match &waiting.message {
                Some(message) => format!("{reason}: {message}"),
                None => reason.to_owned(),
            })
        })
}

/// Polls `check` until it produces a value, or gives up.
async fn wait_for<F, Fut, T>(what: &str, timeout: Duration, mut check: F) -> anyhow::Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = anyhow::Result<Option<T>>>,
{
    let deadline = Instant::now() + timeout;
    loop {
        match check()
            .await
            .with_context(|| format!("failed while waiting for {what}"))?
        {
            Some(value) => return Ok(value),
            None if Instant::now() >= deadline => {
                anyhow::bail!("timed out after {timeout:?} waiting for {what}");
            },
            None => tokio::time::sleep(POLL_INTERVAL).await,
        }
    }
}
