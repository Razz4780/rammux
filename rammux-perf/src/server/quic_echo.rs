//! The QUIC side of the echo server.
//!
//! Separate from the other three, which all arrive as an upgraded HTTP/1.1
//! connection and share [`crate::server::EchoImpl`]. QUIC has no upgrade to
//! arrive on: it is its own transport, its own TLS, and its own listener on a
//! UDP address.
//!
//! It also differs in how it is driven. Every other echo runs its connection
//! in one task, polling a driver alongside its streams, because that is what
//! those protocols require. `quinn` services the socket from the endpoint's
//! own task whatever the caller does, so there is no driver to interleave
//! with, and a task per stream is the honest shape rather than a selector
//! wrapped around futures that would have to be boxed to fit.

use std::{net::SocketAddr, sync::Arc};

use anyhow::Context as _;
use quinn::{Endpoint, crypto::rustls::QuicServerConfig};
use tracing::{Instrument, Level};

use crate::{
    config::QuicMuxerConfig, server::pipe::PipeBytes, stream_util::QuicDuplex, tls::CertBundle,
};

/// Largest control message accepted, which is a small JSON object.
const MAX_CONFIG_BYTES: usize = 4 * 1024;

/// Binds the QUIC listener.
///
/// The transport config comes from the server's own config file rather than
/// from the client, because QUIC's windows and stream limits are transport
/// parameters: they are fixed by the handshake, long before a client could
/// have said anything. What the client sends is checked against this instead;
/// see [`read_and_check_config`].
pub fn endpoint(
    addr: SocketAddr,
    bundle: &CertBundle,
    config: &QuicMuxerConfig,
) -> anyhow::Result<Endpoint> {
    let crypto = QuicServerConfig::try_from(bundle.server_config()?)
        .context("the TLS config cannot be used for QUIC")?;
    let mut server_config = quinn::ServerConfig::with_crypto(Arc::new(crypto));
    server_config.transport_config(Arc::new(config.to_transport_config()));
    Endpoint::server(server_config, addr)
        .with_context(|| format!("failed to bind the QUIC listener on {addr}"))
}

/// Serves one accepted QUIC connection.
#[tracing::instrument(level = Level::INFO, skip_all, fields(peer = %incoming.remote_address()))]
pub async fn handle_connection(incoming: quinn::Incoming, config: Arc<QuicMuxerConfig>) {
    tracing::info!("Handling new QUIC connection");
    match serve(incoming, config).await {
        Ok(()) => tracing::info!("QUIC connection finished"),
        Err(error) => tracing::warn!(error = format!("{error:#}"), "QUIC connection failed"),
    }
}

async fn serve(incoming: quinn::Incoming, config: Arc<QuicMuxerConfig>) -> anyhow::Result<()> {
    let connection = incoming.await.context("QUIC handshake failed")?;
    read_and_check_config(&connection, &config).await?;

    loop {
        let (send, recv) = match connection.accept_bi().await {
            Ok(stream) => stream,
            // The client closing when it is done is how a run ends.
            Err(quinn::ConnectionError::ApplicationClosed(..)) => break Ok(()),
            Err(error) => break Err(anyhow::Error::new(error).context("failed to accept a stream")),
        };
        tokio::spawn(
            async move {
                if let Err(error) = PipeBytes::new(QuicDuplex::new(send, recv)).await {
                    tracing::warn!(error = format!("{error:#}"), "QUIC stream failed");
                }
            }
            .in_current_span(),
        );
    }
}

/// Reads the client's copy of the config and refuses a connection that
/// disagrees with ours.
///
/// This is what stands in for the header the other three protocols carry. It
/// cannot configure anything - by now the handshake has settled the transport
/// parameters - but it can stop a run that would otherwise have measured one
/// side at one window and the other at another, and reported the number as if
/// it meant something.
async fn read_and_check_config(
    connection: &quinn::Connection,
    ours: &QuicMuxerConfig,
) -> anyhow::Result<()> {
    let mut control = connection
        .accept_uni()
        .await
        .context("the client opened no control stream")?;
    let raw = control
        .read_to_end(MAX_CONFIG_BYTES)
        .await
        .context("failed to read the client's config")?;
    let theirs = serde_json::from_slice::<QuicMuxerConfig>(&raw)
        .context("failed to parse the client's config")?;

    if &theirs == ours {
        return Ok(());
    }
    // Said on the wire as well as in the log: the client's error should name
    // the disagreement, not just report a closed connection.
    let message = format!("QUIC config mismatch: server runs {ours:?}, client sent {theirs:?}");
    connection.close(1u32.into(), message.as_bytes());
    Err(anyhow::anyhow!("{message}"))
}
