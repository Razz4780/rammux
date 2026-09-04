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
//!
//! `--dry-run` prints the objects and needs none of this.

use std::{path::PathBuf, time::Duration};

use anyhow::Context as _;
use clap::Args;
use k8s_openapi::api::{
    batch::v1::Job,
    core::v1::{ConfigMap, Namespace, Pod, Secret, Service},
};
use kube::{
    Api, Client,
    api::{DeleteParams, DynamicObject, ListParams, LogParams, PostParams, PropagationPolicy},
    config::{Config, KubeConfigOptions, Kubeconfig},
    discovery,
};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::time::Instant;

use crate::{
    client::Summary,
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

#[derive(Args, Debug)]
pub struct K8sArgs {
    /// Container image with the `rammux-perf` binary as its entrypoint.
    #[arg(long)]
    pub image: String,

    /// Pull policy for that image.
    #[arg(long, default_value = "IfNotPresent")]
    pub image_pull_policy: String,

    /// Namespace to run in. Created if missing, and removed again only if this
    /// run is what created it.
    #[arg(long, default_value = "rammux-perf")]
    pub namespace: String,

    /// Path to a kubeconfig file. Defaults to the usual resolution, and to the
    /// in-cluster environment when running as a pod.
    #[arg(long)]
    pub kubeconfig: Option<PathBuf>,

    /// Kubeconfig context to use.
    #[arg(long)]
    pub context: Option<String>,

    /// Which emulated link to run over.
    #[arg(long, default_value = "datacenter")]
    pub archetype: Archetype,

    /// Path to a JSON file with the muxer configuration - the `muxer` object
    /// of a client config, tagged with its `protocol`.
    #[arg(long)]
    pub muxer_config: PathBuf,

    /// Number of iterations to run.
    #[arg(long, default_value_t = 5)]
    pub iterations: usize,

    /// How many bulk streams each iteration runs.
    #[arg(long, default_value_t = 4)]
    pub bulk_streams: usize,

    /// How much data each bulk stream sends, and reads back, in bytes.
    #[arg(long, default_value_t = 64 * 1024 * 1024)]
    pub bulk_stream_data: usize,

    /// Size of the ping pong message in bytes. Omitted runs no ping pong
    /// stream, and so measures no latency.
    #[arg(long)]
    pub ping_pong_size: Option<usize>,

    /// Encrypt the benchmarked connection with TLS 1.3.
    #[arg(long)]
    pub tls: bool,

    /// How long the client job gets before the run is given up on.
    #[arg(long, default_value_t = 900)]
    pub timeout_secs: u64,

    /// Leave everything in the cluster on exit, to be inspected by hand.
    #[arg(long)]
    pub keep: bool,

    /// Print the objects a run would create, as YAML, and create nothing.
    ///
    /// Needs no cluster and no credentials. Pipe it through
    /// `kubectl apply --dry-run=server -f -` to have a real API server check
    /// it over.
    #[arg(long)]
    pub dry_run: bool,
}

impl K8sArgs {
    fn timeout(&self) -> Duration {
        Duration::from_secs(self.timeout_secs)
    }
}

pub async fn run(args: &K8sArgs) -> anyhow::Result<()> {
    // Everything that can be wrong locally is checked before the cluster is
    // touched, so a typo in the muxer config does not leave objects behind.
    let muxer = read_muxer_config(&args.muxer_config)?;
    let protocol = serde_json::from_value::<MuxerConfig>(muxer.clone())
        .expect("validated above")
        .protocol();

    let naming = Naming::new(args.namespace.clone());
    let profile = args.archetype.profile();

    if args.dry_run {
        return dry_run(args, &naming, &muxer);
    }

    let client = connect(args).await?;

    tracing::info!(
        run_id = naming.run_id,
        namespace = args.namespace,
        archetype = args.archetype.name(),
        protocol,
        rtt_ms = profile.map(|p| p.rtt.as_millis() as u64),
        jitter_ms = profile.map(|p| p.jitter.as_millis() as u64),
        loss_percent = profile.map(|p| p.loss_percent),
        rate_mbit = profile.and_then(|p| p.rate_mbit),
        link_bdp_bytes = profile.and_then(|p| p.bdp_bytes()),
        "Starting a cluster run",
    );

    // Outside the interruptible section: the flag deciding whether teardown
    // may delete the namespace must not be able to disagree with reality.
    let created_namespace = ensure_namespace(&client, &args.namespace).await?;

    let mut signal = ExitSignal::new()?;
    let outcome = tokio::select! {
        () = &mut signal => {
            tracing::warn!("Received an exit signal, tearing down");
            Err(anyhow::anyhow!("interrupted"))
        },
        result = execute(&client, args, &naming, &muxer) => result,
    };

    if args.keep {
        tracing::warn!(
            selector = naming.run_selector(),
            "Leaving every object in the cluster, as asked",
        );
    } else {
        // Deliberately not racing the signal: a second interrupt during
        // teardown would leave the cluster impaired, which is worse than
        // waiting.
        if let Err(error) = teardown(&client, args, &naming, created_namespace).await {
            tracing::error!(
                error = format!("{error:#}"),
                selector = naming.run_selector(),
                "Teardown failed, objects may be left behind",
            );
        }
    }

    let report = outcome?;
    report.print(args, protocol);
    Ok(())
}

/// Creates everything, waits for it, and reads the result out of the job.
async fn execute(
    client: &Client,
    args: &K8sArgs,
    naming: &Naming,
    muxer: &Value,
) -> anyhow::Result<RunReport> {
    let ns = &args.namespace;

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
        .context("failed to create the TLS secret")?;

    let config_maps = Api::<ConfigMap>::namespaced(client.clone(), ns);
    config_maps
        .create(
            &PostParams::default(),
            &resources::config_map(
                naming.server_config_map(),
                naming,
                &resources::server_config(muxer),
            )?,
        )
        .await
        .context("failed to create the server config map")?;

    let pods = Api::<Pod>::namespaced(client.clone(), ns);
    pods.create(
        &PostParams::default(),
        &resources::server_pod(naming, args)?,
    )
    .await
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
                &resources::client_config(naming, args, &server_ip, muxer),
            )?,
        )
        .await
        .context("failed to create the client config map")?;

    // The client starts before the impairment and waits at the start gate for
    // it. Chaos Mesh needs a running container to inject into, and the client
    // must not connect until it has - the gate is what makes those two
    // compatible, and it is why the client is created here rather than last.
    let jobs = Api::<Job>::namespaced(client.clone(), ns);
    jobs.create(
        &PostParams::default(),
        &resources::client_job(naming, args)?,
    )
    .await
    .context("failed to create the client job")?;
    wait_for("the client pod to be running", SETUP_TIMEOUT, || async {
        let Some(pod) = client_pod(client, args, naming).await? else {
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

    if let Some(profile) = args.archetype.profile() {
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
        let chaos_duration = args.timeout() + Duration::from_secs(120);
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
            archetype = args.archetype.name(),
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

    let succeeded = wait_for("the client job to finish", args.timeout(), || async {
        let status = jobs
            .get(&naming.client_job())
            .await?
            .status
            .unwrap_or_default();
        if status.succeeded.unwrap_or(0) > 0 {
            return Ok(Some(true));
        }
        if status.failed.unwrap_or(0) > 0 {
            return Ok(Some(false));
        }
        // A job whose pod cannot start never succeeds and never fails; it
        // would only ever hit the timeout, which says nothing useful.
        if let Some(pod) = client_pod(client, args, naming).await?
            && let Some(reason) = stuck_reason(&pod.status.unwrap_or_default())
        {
            anyhow::bail!("the client pod cannot start: {reason}");
        }
        Ok(None)
    })
    .await?;

    // Logs are read whether the job passed or failed: a failed run's warnings
    // are the whole reason to look.
    let logs = client_logs(client, args, naming).await?;
    let iterations = parse_iterations(&logs);
    if !succeeded {
        anyhow::bail!(
            "the client job failed; its last log line was: {}",
            logs.lines().last().unwrap_or("(no output)")
        );
    }
    Ok(RunReport {
        archetype: args.archetype,
        iterations,
    })
}

/// Prints what a run would create, without a cluster or credentials.
///
/// The client's config normally names the server pod's address, which is only
/// known once that pod is running, so here it gets a placeholder.
fn dry_run(args: &K8sArgs, naming: &Naming, muxer: &Value) -> anyhow::Result<()> {
    const PLACEHOLDER_IP: &str = "0.0.0.0";

    let mut documents = vec![
        serde_yaml_value(&resources::config_map(
            naming.server_config_map(),
            naming,
            &resources::server_config(muxer),
        )?)?,
        serde_yaml_value(&resources::server_pod(naming, args)?)?,
        serde_yaml_value(&resources::config_map(
            naming.client_config_map(),
            naming,
            &resources::client_config(naming, args, PLACEHOLDER_IP, muxer),
        )?)?,
        serde_yaml_value(&resources::client_job(naming, args)?)?,
    ];
    if let Some(profile) = args.archetype.profile() {
        let duration = args.timeout() + Duration::from_secs(120);
        documents.push(serde_yaml_value(&resources::netem_chaos(
            naming, &profile, duration,
        )?)?);
        if let Some(rate_mbit) = profile.rate_mbit {
            documents.push(serde_yaml_value(&resources::bandwidth_chaos(
                naming, &profile, rate_mbit, duration,
            )?)?);
        }
    }
    documents.push(serde_yaml_value(&resources::gate_service(naming)?)?);

    println!(
        "# rammux-perf run {}, archetype {}",
        naming.run_id,
        args.archetype.name()
    );
    println!("# the TLS secret is omitted: its contents are generated per run");
    println!(
        "# the client's server_addr is a placeholder, filled in once the server pod has an IP"
    );
    for document in documents {
        println!("---");
        print!("{document}");
    }
    Ok(())
}

/// Serializes an object the way `kubectl` would show it.
///
/// JSON is valid YAML, so this needs no YAML dependency; it is pretty-printed
/// so the output is readable rather than one line per object.
fn serde_yaml_value<T: serde::Serialize>(object: &T) -> anyhow::Result<String> {
    let mut rendered = serde_json::to_string_pretty(object)?;
    rendered.push('\n');
    Ok(rendered)
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
async fn client_pod(
    client: &Client,
    args: &K8sArgs,
    naming: &Naming,
) -> anyhow::Result<Option<Pod>> {
    let listed = Api::<Pod>::namespaced(client.clone(), &args.namespace)
        .list(&ListParams::default().labels(&format!(
            "{},{COMPONENT_LABEL}=client",
            naming.run_selector()
        )))
        .await
        .context("failed to look for the client pod")?;
    Ok(listed.items.into_iter().next())
}

/// Everything the client job wrote.
async fn client_logs(client: &Client, args: &K8sArgs, naming: &Naming) -> anyhow::Result<String> {
    let pod = client_pod(client, args, naming)
        .await?
        .context("the client job produced no pod")?;
    let name = pod
        .metadata
        .name
        .as_deref()
        .context("client pod has no name")?;
    Api::<Pod>::namespaced(client.clone(), &args.namespace)
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

/// One `Iteration finished` event, as the client logs it.
///
/// The percentile fields are optional because an iteration that completed no
/// ping pong exchange summarises to `NaN`, which JSON has no spelling for.
#[derive(Debug, Deserialize)]
struct Iteration {
    iteration: usize,
    attempt: usize,
    elapsed_ms: u64,
    cpu_ms: u64,
    bulk_streams: usize,
    #[serde(default)]
    bulk_mbps_mean: Option<f64>,
    #[serde(default)]
    bulk_mbps_p50: Option<f64>,
    #[serde(default)]
    bulk_mbps_p99: Option<f64>,
    ping_pong_count: usize,
    #[serde(default)]
    latency_ms_mean: Option<f64>,
    #[serde(default)]
    latency_ms_p50: Option<f64>,
    #[serde(default)]
    latency_ms_p99: Option<f64>,
}

/// Picks the iteration reports out of the job's JSON log.
///
/// Anything unparseable is skipped rather than fatal: the log also carries the
/// startup line, retry warnings and whatever the runtime had to say, and a
/// summary is still worth printing when one line is malformed.
fn parse_iterations(logs: &str) -> Vec<Iteration> {
    logs.lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .filter(|line| line["fields"]["message"] == "Iteration finished")
        .filter_map(|line| serde_json::from_value(line["fields"].clone()).ok())
        .collect()
}

/// What a finished run has to say.
struct RunReport {
    archetype: Archetype,
    iterations: Vec<Iteration>,
}

impl RunReport {
    fn print(&self, args: &K8sArgs, protocol: &str) {
        let retried = self.iterations.iter().filter(|it| it.attempt > 1).count();
        let throughput = Summary::of(self.iterations.iter().filter_map(|it| it.bulk_mbps_mean));
        let latency = Summary::of(self.iterations.iter().filter_map(|it| it.latency_ms_mean));
        let latency_p99 = Summary::of(self.iterations.iter().filter_map(|it| it.latency_ms_p99));
        let elapsed = Summary::of(self.iterations.iter().map(|it| it.elapsed_ms as f64));
        let cpu = Summary::of(self.iterations.iter().map(|it| it.cpu_ms as f64));
        let exchanges: usize = self.iterations.iter().map(|it| it.ping_pong_count).sum();

        let profile = self.archetype.profile();
        println!();
        println!("{protocol} over {}", self.archetype.name());
        match profile {
            Some(profile) => println!(
                "  link          {:?} rtt, {:?} jitter, {}% loss, {}",
                profile.rtt,
                profile.jitter,
                profile.loss_percent,
                match profile.rate_mbit {
                    Some(rate) => format!("{rate} Mbit/s"),
                    None => "unshaped".to_owned(),
                },
            ),
            None => println!("  link          unimpaired (control)"),
        }
        if let Some(bdp) = profile.and_then(|profile| profile.bdp_bytes()) {
            println!("  link bdp      {bdp} bytes");
        }
        println!(
            "  workload      {} bulk streams x {} bytes, ping pong {}",
            args.bulk_streams,
            args.bulk_stream_data,
            match args.ping_pong_size {
                Some(size) => format!("{size} bytes"),
                None => "off".to_owned(),
            },
        );
        println!(
            "  iterations    {} of {} requested ({retried} needed a retry)",
            self.iterations.len(),
            args.iterations,
        );

        // Per iteration first, because a summary hides the one iteration that
        // went wrong, and on an impaired link that is usually the interesting
        // one. Within an iteration, `p50`/`p99` are across the bulk streams
        // and across the ping pong exchanges respectively.
        println!();
        println!(
            "  {:>4} {:>4} {:>7} {:>7} {:>9} {:>9} {:>9} {:>6} {:>8} {:>8} {:>8}",
            "iter",
            "try",
            "elapsed",
            "cpu",
            "thr mean",
            "thr p50",
            "thr p99",
            "pp n",
            "lat mean",
            "lat p50",
            "lat p99",
        );
        for it in &self.iterations {
            println!(
                "  {:>4} {:>4} {:>7} {:>7} {:>9} {:>9} {:>9} {:>6} {:>8} {:>8} {:>8}",
                it.iteration,
                it.attempt,
                it.elapsed_ms,
                it.cpu_ms,
                optional(it.bulk_mbps_mean, 0),
                optional(it.bulk_mbps_p50, 0),
                optional(it.bulk_mbps_p99, 0),
                it.ping_pong_count,
                optional(it.latency_ms_mean, 3),
                optional(it.latency_ms_p50, 3),
                optional(it.latency_ms_p99, 3),
            );
        }

        println!();
        println!("  across the {} iterations:", self.iterations.len());
        println!("                        mean         p50         p99");
        print_row("  throughput mbit/s ", &throughput);
        print_row("  latency ms        ", &latency);
        print_row("  latency ms p99    ", &latency_p99);
        print_row("  elapsed ms        ", &elapsed);
        print_row("  cpu ms            ", &cpu);
        println!();
        println!(
            "  {} bulk streams per iteration, {exchanges} ping pong exchanges in total",
            self.iterations
                .first()
                .map(|it| it.bulk_streams)
                .unwrap_or_default(),
        );
        println!(
            "  (throughput is per bulk stream; every column summarises the per-iteration means)",
        );
    }
}

fn print_row(label: &str, summary: &Summary) {
    println!(
        "{label}{:>11.1} {:>11.1} {:>11.1}",
        summary.mean, summary.p50, summary.p99
    );
}

/// A measurement the client had no sample for prints as `-`, not as `NaN`.
fn optional(value: Option<f64>, precision: usize) -> String {
    match value {
        Some(value) => format!("{value:.precision$}"),
        None => "-".to_owned(),
    }
}

/// Reads the muxer config, failing here rather than in the cluster.
fn read_muxer_config(path: &PathBuf) -> anyhow::Result<Value> {
    let raw = std::fs::read(path)
        .with_context(|| format!("failed to read the muxer config at {}", path.display()))?;
    let value: Value = serde_json::from_slice(&raw)
        .with_context(|| format!("failed to parse the muxer config at {}", path.display()))?;
    serde_json::from_value::<MuxerConfig>(value.clone()).with_context(|| {
        format!(
            "the muxer config at {} is not a valid muxer configuration",
            path.display()
        )
    })?;
    Ok(value)
}

async fn connect(args: &K8sArgs) -> anyhow::Result<Client> {
    let options = KubeConfigOptions {
        context: args.context.clone(),
        ..Default::default()
    };
    let config = match &args.kubeconfig {
        Some(path) => {
            let kubeconfig = Kubeconfig::read_from(path)
                .with_context(|| format!("failed to read kubeconfig at {}", path.display()))?;
            Config::from_custom_kubeconfig(kubeconfig, &options)
                .await
                .context("failed to build a client from the given kubeconfig")?
        },
        None => Config::from_kubeconfig(&options)
            .await
            .or_else(|error| {
                // In a pod there is no kubeconfig, and the service account is
                // the right thing to fall back to.
                Config::incluster().map_err(|_| error)
            })
            .context("failed to find cluster credentials")?,
    };
    Client::try_from(config).context("failed to build a Kubernetes client")
}

/// Returns whether this run is what created the namespace.
async fn ensure_namespace(client: &Client, name: &str) -> anyhow::Result<bool> {
    let namespaces = Api::<Namespace>::all(client.clone());
    if namespaces.get_opt(name).await?.is_some() {
        return Ok(false);
    }
    let namespace: Namespace = serde_json::from_value(json!({
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": { "name": name, "labels": { "app.kubernetes.io/name": "rammux-perf" } },
    }))?;
    namespaces
        .create(&PostParams::default(), &namespace)
        .await
        .with_context(|| format!("failed to create namespace {name}"))?;
    tracing::info!(namespace = name, "Created the namespace");
    Ok(true)
}

/// Removes everything this run made.
///
/// Deletes by the run label rather than by name, so it removes what is there
/// rather than what this process remembers creating. Every kind is attempted
/// even if an earlier one fails, because a leftover NetworkChaos is much worse
/// than a leftover config map.
async fn teardown(
    client: &Client,
    args: &K8sArgs,
    naming: &Naming,
    created_namespace: bool,
) -> anyhow::Result<()> {
    let ns = &args.namespace;
    let selector = ListParams::default().labels(&naming.run_selector());
    let params = DeleteParams {
        propagation_policy: Some(PropagationPolicy::Background),
        ..Default::default()
    };
    let mut failures = Vec::new();

    // The impairment goes first: everything after this talks to the cluster,
    // and there is no reason to do it over a link we broke on purpose.
    if args.archetype.profile().is_some()
        && let Ok((api_resource, _)) = discovery::pinned_kind(
            client,
            &kube::core::GroupVersionKind::gvk("chaos-mesh.org", "v1alpha1", "NetworkChaos"),
        )
        .await
    {
        let chaos: Api<DynamicObject> = Api::namespaced_with(client.clone(), ns, &api_resource);
        if let Err(error) = chaos.delete_collection(&params, &selector).await {
            failures.push(format!("NetworkChaos: {error}"));
        }
    }

    macro_rules! delete {
        ($kind:ty, $what:literal) => {
            if let Err(error) = Api::<$kind>::namespaced(client.clone(), ns)
                .delete_collection(&params, &selector)
                .await
            {
                failures.push(format!("{}: {error}", $what));
            }
        };
    }
    delete!(Job, "jobs");
    delete!(Pod, "pods");
    delete!(Service, "services");
    delete!(ConfigMap, "config maps");
    delete!(Secret, "secrets");

    if created_namespace
        && let Err(error) = Api::<Namespace>::all(client.clone())
            .delete(ns, &params)
            .await
    {
        failures.push(format!("namespace: {error}"));
    }

    if failures.is_empty() {
        tracing::info!(
            run_id = naming.run_id,
            "Removed everything this run created"
        );
        Ok(())
    } else {
        Err(anyhow::anyhow!("{}", failures.join("; ")))
    }
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
