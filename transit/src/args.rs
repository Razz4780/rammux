//! Protocol settings, shared by both ends and by the harness that starts them.

use std::time::Duration;

use clap::{Args, ValueEnum};

use transit::{Config, Growth, Role, Sizing};

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum Mode {
    /// Straight through the socket, no protocol.
    ///
    /// The floor for latency-without-a-window and the ceiling for throughput.
    /// Every windowed result is a trade against it.
    Raw,
    Transit,
}

/// Settings both ends need to agree on to be comparable.
#[derive(Args, Debug, Clone, Copy)]
pub struct ProtocolArgs {
    /// Whether to run the transit window at all.
    #[arg(long, value_enum, default_value = "transit")]
    pub mode: Mode,
    /// Window granted before anything is measured, in bytes.
    #[arg(long, default_value_t = Sizing::default().initial)]
    pub window: u32,
    /// Growth limit, in bytes.
    #[arg(long, default_value_t = Sizing::default().max)]
    pub max_window: u32,
    /// Freed credit that triggers a re-grant, in bytes.
    ///
    /// Capped at half the window, so a window smaller than twice this keeps
    /// the classic half-window cadence.
    #[arg(long, default_value_t = Sizing::default().re_grant)]
    pub re_grant: u32,
    /// How many re-grants to fit into a round trip, or 0 for a flat
    /// `--re-grant` threshold on every link.
    #[arg(long, default_value_t = Sizing::default().re_grants_per_rtt)]
    pub re_grants_per_rtt: u32,
    /// One-way queuing delay the `ledbat` rule holds, in milliseconds.
    ///
    /// This is the standing queue the window is allowed to keep, so it is
    /// also, near enough, what it adds to every byte's latency.
    #[arg(long, default_value_t = 5.0)]
    pub target_queue_ms: f64,
    /// The same target as a fraction of the clean round trip, which takes
    /// precedence when non-zero.
    ///
    /// Measured, more queue than about 0.3 of the round trip stops buying
    /// throughput on every link tried, and a flat target can only sit at that
    /// knee on one of them. Lower this to trade throughput back for latency:
    /// 0.20 costs about 1.5 points of link and saves about 30 ms across the
    /// four, and `--target-queue-rtts 0 --target-queue-ms 5` is the
    /// latency-first end at about 91% of link.
    #[arg(long, default_value_t = 0.30)]
    pub target_queue_rtts: f64,
    /// Fraction of the window a full-scale delay error moves it by, per round
    /// trip.
    /// Measured: 0.5 oscillates the window between a third and twice its
    /// settling point, because a change takes a round trip to show up in the
    /// delay it is steering.
    #[arg(long, default_value_t = 0.1)]
    pub ledbat_gain: f64,
    /// How many control intervals the delay signal's median is taken over.
    ///
    /// One interval's minimum swings by an order of magnitude between round
    /// trips on a slow link, because a window-limited sender either leaves a
    /// gap in an interval or does not.
    #[arg(long, default_value_t = Sizing::default().delay_filter)]
    pub delay_filter: usize,
    /// How many of the last probe's durations to wait before the next one.
    ///
    /// Spacing exchanges by their own duration bounds the share of the
    /// connection's time spent probing on any link at once, where a fixed
    /// interval could only be tuned to some of them. LEDBAT++ uses 9 for the
    /// same schedule, but measures a slowdown *through* the regrowth back to
    /// the old window; ours ends the moment output resumes, before the empty
    /// pipe has refilled, so the multiplier has to carry that difference.
    /// Measured: 128 settles at 6 to 16 s on the links tried and matches a
    /// fixed 8 s interval's throughput on every one of them; 32 and below
    /// cost 1 to 2 points of link, 256 runs into the ceiling on long paths.
    #[arg(long, default_value_t = transit::DEFAULT_PROBE_SPACING)]
    pub probe_spacing: f64,
    /// Pin `SO_SNDBUF` to this many bytes, turning the kernel's autotuning
    /// off. Unset leaves autotuning on; either way the result is recorded.
    #[arg(long)]
    pub send_buffer: Option<usize>,
}

impl ProtocolArgs {
    pub fn config(&self, role: Role) -> Config {
        Config {
            sizing: Sizing {
                initial: self.window,
                max: self.max_window.max(self.window),
                re_grant: self.re_grant.max(1),
                re_grants_per_rtt: self.re_grants_per_rtt,
                delay_filter: self.delay_filter.max(1),
                growth: Growth::Ledbat {
                    target_rtts: self.target_queue_rtts,
                    target: Duration::from_micros((self.target_queue_ms * 1000.0) as u64),
                    gain: self.ledbat_gain,
                },
            },
            probe_spacing: self.probe_spacing,
            role,
        }
    }

    /// One line naming every knob, so a log says what produced it.
    pub fn describe(&self) -> String {
        format!(
            "mode={:?} window={} max_window={} re_grant={} re_grants_per_rtt={} \
             target_queue_ms={} target_queue_rtts={} ledbat_gain={} \
             delay_filter={} probe_spacing={} \
             send_buffer={:?}",
            self.mode,
            self.window,
            self.max_window,
            self.re_grant,
            self.re_grants_per_rtt,
            self.target_queue_ms,
            self.target_queue_rtts,
            self.ledbat_gain,
            self.delay_filter,
            self.probe_spacing,
            self.send_buffer,
        )
    }

    /// The same settings as a command line, for the harness to hand to the
    /// client and the server it starts.
    pub fn to_argv(self) -> Vec<String> {
        let mut argv = vec![
            "--mode".to_string(),
            match self.mode {
                Mode::Raw => "raw",
                Mode::Transit => "transit",
            }
            .to_string(),
            "--window".to_string(),
            self.window.to_string(),
            "--max-window".to_string(),
            self.max_window.to_string(),
            "--re-grant".to_string(),
            self.re_grant.to_string(),
            "--re-grants-per-rtt".to_string(),
            self.re_grants_per_rtt.to_string(),
            "--target-queue-ms".to_string(),
            self.target_queue_ms.to_string(),
            "--target-queue-rtts".to_string(),
            self.target_queue_rtts.to_string(),
            "--ledbat-gain".to_string(),
            self.ledbat_gain.to_string(),
            "--delay-filter".to_string(),
            self.delay_filter.to_string(),
            "--probe-spacing".to_string(),
            self.probe_spacing.to_string(),
        ];
        if let Some(bytes) = self.send_buffer {
            argv.push("--send-buffer".to_string());
            argv.push(bytes.to_string());
        }
        argv
    }
}

#[cfg(test)]
mod test {
    use clap::Parser;

    use super::*;

    #[derive(Parser)]
    struct Cli {
        #[command(flatten)]
        protocol: ProtocolArgs,
    }

    /// The command line's defaults are the tuned configuration and nothing
    /// else. The sizing scalars are read straight from `Sizing::default`, but
    /// the delay rule's own knobs are separate flags and can only be pinned to
    /// it here.
    /// The rule's knobs are separate flags that `config` has to carry over by
    /// hand. They were once parsed, printed, forwarded - and never read.
    #[test]
    fn the_delay_rule_flags_reach_the_config() {
        let cli = Cli::parse_from([
            "transit",
            "--ledbat-gain",
            "0.3",
            "--target-queue-rtts",
            "0.2",
            "--target-queue-ms",
            "7",
        ]);
        assert_eq!(
            cli.protocol.config(Role::Initiator).sizing.growth,
            Growth::Ledbat {
                target_rtts: 0.2,
                target: Duration::from_millis(7),
                gain: 0.3,
            },
            "a flag was parsed but did not reach the window"
        );
    }

    #[test]
    fn the_flags_default_to_the_tuned_sizing() {
        let cli = Cli::parse_from(["transit"]);
        let config = cli.protocol.config(Role::Initiator);
        assert_eq!(
            config.sizing,
            Sizing::default(),
            "a flag's default has drifted from Sizing::default"
        );
        assert_eq!(
            config.probe_spacing,
            transit::DEFAULT_PROBE_SPACING,
            "the probe spacing flag has drifted from the library's default"
        );
    }
}
