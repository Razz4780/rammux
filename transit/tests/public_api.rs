//! What a downstream crate can actually reach.
//!
//! The unit tests live inside the crate and can see everything; this one is a
//! separate crate, so it compiles only against the published surface. It is
//! here to catch a type that quietly became unnameable from outside, which an
//! in-crate test cannot.

use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt, duplex};
use transit::{Config, Growth, Role, Sizing, Transit};

/// The configuration surface, reached only through public paths - and then
/// actually applied, so a field that stopped reaching the connection shows up.
#[tokio::test]
async fn a_configuration_can_be_built_adjusted_and_applied_from_outside() {
    let mut config = Config::new(Role::Initiator);
    assert_eq!(config.sizing, Sizing::default());
    assert_eq!(config.probe_spacing, transit::DEFAULT_PROBE_SPACING);

    config.sizing.initial = 48 * 1024;
    config.sizing.max = 4 * 1024 * 1024;
    config.sizing.growth = Growth::Ledbat {
        target_rtts: 0.25,
        target: Duration::from_millis(5),
        gain: 0.2,
    };

    let (near, _far) = duplex(1024);
    let connection = Transit::new(near, config);
    assert_eq!(
        connection.stats().window,
        48 * 1024,
        "the configured initial window did not reach the connection"
    );
}

/// Payload in one end and out the other, through nothing but the public API.
///
/// Chunks are [`transit::MAX_PAYLOAD`], so every write is the largest single
/// frame the wire format allows.
#[tokio::test]
async fn a_transfer_completes_over_the_public_api() {
    let chunk = vec![0xAB; transit::MAX_PAYLOAD as usize];
    let total = 8 * chunk.len();

    let (near, far) = duplex(16 * 1024);
    let mut sender = Transit::new(near, Config::new(Role::Initiator));
    let mut receiver = Transit::new(far, Config::new(Role::Responder));

    let writer = tokio::spawn(async move {
        for _ in 0..8 {
            sender.write_all(&chunk).await.unwrap();
        }
        sender.shutdown().await.unwrap();
        // `stats` and `get_ref` are the observability surface.
        assert!(
            sender.stats().stalls > 0,
            "a window smaller than the transfer never bound the sender"
        );
        let _: &tokio::io::DuplexStream = sender.get_ref();
    });

    let mut received = Vec::new();
    receiver.read_to_end(&mut received).await.unwrap();
    writer.await.unwrap();

    assert_eq!(received.len(), total);
    assert!(received.iter().all(|byte| *byte == 0xAB));
}
