//! Link archetypes: the impairment profiles a cluster run is measured against.
//!
//! Flow control exists to cope with a path that has delay, jitter, loss and a
//! bottleneck rate, so the numbers below are the point of the whole exercise.
//! Every one of them is sourced rather than invented; the per-archetype docs
//! say where from. Where a source gives a range, the value picked sits in the
//! middle of it, because an archetype is meant to be representative, not
//! worst case.
//!
//! # Sources
//!
//! * Cross-AZ latency in one cloud region: the AWS Well-Architected Framework
//!   works from 1.5 ms at p50, and independent per-region measurements put the
//!   p50 between 0.39 ms (`ap-northeast-3b` to `ap-northeast-3c`) and 2.42 ms
//!   (`sa-east-1a` to `sa-east-1b`).
//! * Wide-area loss and round trip: a study of bulk transfer over research and
//!   commodity paths reports 0.5% average loss with a 30-60 ms round trip
//!   within the US, Asia and Europe, and 1.4% average loss with 200-600 ms
//!   between continents.
//! * Wi-Fi: measured 802.11 round trips of 3-5 ms, with jitter below 1 ms for
//!   802.11n at short range rising to 18 ms at range - the variance, not the
//!   mean, is what makes Wi-Fi hard on a congestion controller.
//! * VPN cost, which is two separate things and easy to conflate. The
//!   *crypto* is cheap: WireGuard measured at 0.3 ms added latency and a
//!   95 Mbit/s ceiling in one comparison, against OpenVPN's 2.1 ms and
//!   81 Mbit/s, and a multi-site deployment saw the average rise from 18.4 ms
//!   to 19.8 ms. The *detour* is what costs, because traffic goes via a
//!   concentrator instead of straight to the service: 5-30 ms for a nearby,
//!   well connected server, more when it is not, and a full tunnel that has
//!   become the bottleneck shows up as 40 ms or more against a split one.
//!   The rate ceiling comes from the crypto, the delay almost entirely from
//!   the detour.
//! * Mobile broadband loss: at least half of connections stay under 1% loss,
//!   but 15-43% of connections on the better operators exceed 0.5%, and above
//!   2% is the accepted threshold for a path being in trouble.

use std::time::Duration;

use clap::ValueEnum;

/// Which emulated link to run the benchmark over.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
#[clap(rename_all = "kebab-case")]
pub enum Archetype {
    /// No impairment: the cluster's own network, as a control.
    None,
    /// Two pods in one cloud region, across availability zones.
    Datacenter,
    /// A healthy intra-continental internet path.
    Wan,
    /// An intercontinental path with the loss and delay to match.
    LossyWan,
    /// An office Wi-Fi client reaching the service through a VPN.
    WifiVpn,
    /// A remote site reaching the service across a continent and a VPN.
    WanVpn,
}

impl Archetype {
    /// The impairment this archetype stands for, or [`None`] for the control.
    pub fn profile(self) -> Option<LinkProfile> {
        let profile = match self {
            // The control. Whatever the cluster's own pod network does is
            // what the run measures, which is the baseline every impaired
            // run is compared against.
            Self::None => return None,

            // Cross-AZ inside one region: 1 ms round trip sits between the
            // 1.5 ms the AWS guidance assumes and the sub-millisecond figures
            // measured in the faster regions. Loss at this scale is a fault,
            // not a property of the link, so it stays at zero, and the rate
            // is left unshaped - a modern instance gets several Gbit/s and
            // the bottleneck is the host, which is what we want to measure.
            Self::Datacenter => LinkProfile {
                rtt: Duration::from_millis(1),
                jitter: Duration::from_micros(200),
                delay_correlation: 0,
                loss_percent: 0.0,
                loss_correlation: 0,
                rate_mbit: None,
            },

            // A healthy path within a continent: the 30-60 ms band, taken at
            // its top because the interesting case is the longer one, with
            // the 0.5% loss that comes with it. 200 Mbit/s is a well
            // provisioned site uplink, and enough that the window, not the
            // rate, is what limits a single connection.
            Self::Wan => LinkProfile {
                rtt: Duration::from_millis(60),
                jitter: Duration::from_millis(5),
                delay_correlation: 25,
                loss_percent: 0.5,
                loss_correlation: 25,
                rate_mbit: Some(200),
            },

            // Intercontinental: 200 ms is the bottom of the 200-600 ms band,
            // and 1.5% is just above the 1.4% average reported for those
            // paths and inside the 1-2.5% range still called acceptable.
            // This is the archetype the transit window exists for.
            Self::LossyWan => LinkProfile {
                rtt: Duration::from_millis(200),
                jitter: Duration::from_millis(30),
                delay_correlation: 25,
                loss_percent: 1.5,
                loss_correlation: 25,
                rate_mbit: Some(50),
            },

            // Office Wi-Fi, out through a VPN concentrator, on to the
            // service. 4 ms of Wi-Fi access + 15 ms of detour, in the middle
            // of the 5-30 ms band for a nearby concentrator, + ~1 ms of
            // crypto. The delay is unremarkable and the variation is the
            // point: Wi-Fi jitter reaches 18 ms at range, so 12 ms here
            // swings the round trip between 8 and 32 ms, and a congestion
            // controller that mistakes that for congestion will behave badly.
            // 100 Mbit/s is where WireGuard tops out in the measurements.
            Self::WifiVpn => LinkProfile {
                rtt: Duration::from_millis(20),
                jitter: Duration::from_millis(12),
                delay_correlation: 25,
                loss_percent: 0.5,
                loss_correlation: 25,
                rate_mbit: Some(100),
            },

            // A remote site on a full tunnel: the 60 ms intra-continental
            // path + 25 ms of detour, since a full tunnel hauls everything
            // via the concentrator and shows up as 40 ms or more against a
            // split one, + ~1 ms of crypto. Longer than `wifi-vpn` because
            // the distance dominates - the tunnel adds much the same either
            // way, and a continent is worth more delay than a wireless hop.
            // Loss is the path's 0.5%; the tunnel has no measured loss of
            // its own to add.
            Self::WanVpn => LinkProfile {
                rtt: Duration::from_millis(85),
                jitter: Duration::from_millis(10),
                delay_correlation: 25,
                loss_percent: 0.5,
                loss_correlation: 25,
                rate_mbit: Some(100),
            },
        };
        Some(profile)
    }

    /// The name this archetype is selected by, for logs and the summary.
    pub fn name(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Datacenter => "datacenter",
            Self::Wan => "wan",
            Self::LossyWan => "lossy-wan",
            Self::WifiVpn => "wifi-vpn",
            Self::WanVpn => "wan-vpn",
        }
    }
}

/// What an archetype does to the path between the client and the server.
#[derive(Debug, Clone, Copy)]
pub struct LinkProfile {
    /// Round trip over the whole path.
    ///
    /// Split across the two directions - see [`LinkProfile::one_way_delay`].
    pub rtt: Duration,
    /// Delay variation over the whole round trip.
    pub jitter: Duration,
    /// How strongly each packet's delay follows the previous one, in percent.
    ///
    /// Zero makes the delay independent per packet, which is whiter than any
    /// real path; the wide-area profiles use the 25% that netem's own
    /// internet-path examples do.
    pub delay_correlation: u32,
    /// Packet loss, in percent.
    pub loss_percent: f64,
    /// How strongly loss clusters, in percent. Real loss arrives in bursts.
    pub loss_correlation: u32,
    /// Bottleneck rate in Mbit/s, or [`None`] to leave the rate unshaped.
    pub rate_mbit: Option<u32>,
}

impl LinkProfile {
    /// The delay to give Chaos Mesh, which applies it to each pod's egress.
    ///
    /// Both ends are impaired, so each leg carries half the round trip.
    pub fn one_way_delay(&self) -> Duration {
        self.rtt / 2
    }

    /// The jitter to give Chaos Mesh, per direction.
    ///
    /// netem draws uniformly from +/- the jitter it is given, and a round trip
    /// crosses two of those draws. Their sum spans +/- twice one draw, so half
    /// the round trip's jitter per leg is what reproduces it end to end - and
    /// the sum is triangular rather than uniform, which is closer to a real
    /// path than either leg is on its own.
    pub fn one_way_jitter(&self) -> Duration {
        self.jitter / 2
    }

    /// Bandwidth-delay product in bytes, or [`None`] on an unshaped link.
    ///
    /// Not used to configure anything - it is logged, because it is the
    /// number a receive window has to reach for the link to stay full, and
    /// having it next to the results saves working it out by hand.
    pub fn bdp_bytes(&self) -> Option<u64> {
        let rate_mbit = u64::from(self.rate_mbit?);
        Some(rate_mbit * 1_000_000 / 8 * self.rtt.as_micros() as u64 / 1_000_000)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    /// The archetypes are meant to be a ladder, and the whole point of one is
    /// that it is ordered. An RTT that is out of place - a VPN'd wireless hop
    /// costing more than a continent, say - means a number was invented
    /// rather than derived, which is how the VPN profiles went wrong once.
    #[test]
    fn the_archetypes_are_a_ladder() {
        let ladder = [
            Archetype::Datacenter,
            Archetype::WifiVpn,
            Archetype::Wan,
            Archetype::WanVpn,
            Archetype::LossyWan,
        ];
        let rtts: Vec<Duration> = ladder
            .iter()
            .map(|archetype| archetype.profile().expect("only `none` has no profile").rtt)
            .collect();
        assert!(
            rtts.is_sorted(),
            "round trips are out of order: {:?}",
            ladder
                .iter()
                .map(|a| a.name())
                .zip(&rtts)
                .collect::<Vec<_>>(),
        );
    }

    /// Adding a VPN to a path costs delay and caps the rate; it never makes
    /// either better. Cheap to state, and it is exactly the comparison that
    /// was wrong.
    #[test]
    fn a_vpn_only_ever_costs() {
        for (bare, tunnelled) in [
            (Archetype::Wan, Archetype::WanVpn),
            // Wi-Fi alone is not an archetype, so this stands in for it: even
            // a local wireless client behind a VPN beats the datacenter.
            (Archetype::Datacenter, Archetype::WifiVpn),
        ] {
            let bare_profile = bare.profile().unwrap();
            let tunnelled_profile = tunnelled.profile().unwrap();
            assert!(
                tunnelled_profile.rtt > bare_profile.rtt,
                "{} should be slower than {}",
                tunnelled.name(),
                bare.name(),
            );
            let capped = tunnelled_profile.rate_mbit.expect("a tunnel has a ceiling");
            assert!(
                bare_profile.rate_mbit.is_none_or(|bare| capped <= bare),
                "{} should not be faster than {}",
                tunnelled.name(),
                bare.name(),
            );
        }
    }

    /// Jitter that exceeds the delay would let netem produce a negative one.
    #[test]
    fn jitter_never_swallows_the_delay() {
        for archetype in [
            Archetype::Datacenter,
            Archetype::Wan,
            Archetype::LossyWan,
            Archetype::WifiVpn,
            Archetype::WanVpn,
        ] {
            let profile = archetype.profile().unwrap();
            assert!(
                profile.jitter < profile.rtt,
                "{} jitter {:?} is not below its {:?} round trip",
                archetype.name(),
                profile.jitter,
                profile.rtt,
            );
        }
    }
}
