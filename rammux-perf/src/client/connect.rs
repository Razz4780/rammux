//! Getting onto the server: TCP, optional TLS, and the HTTP/1.1 upgrade that
//! hands the connection over to the multiplexer.

use anyhow::Context;
use bytes::Bytes;
use http_body_util::{BodyExt, Empty};
use hyper::{
    Method, Request, StatusCode,
    header::{CONNECTION, HOST, UPGRADE},
    upgrade::Upgraded,
};
use hyper_util::rt::TokioIo;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
};

use crate::{
    config::{ClientConfig, MuxerConfig},
    tls,
};

/// Connects to the server and upgrades the connection to `protocol`.
///
/// `muxer_config` carries the multiplexer's configuration serialized to JSON and encoded with base64.
pub async fn connect(
    config: &ClientConfig,
    protocol: &str,
    muxer_config: Vec<u8>,
) -> anyhow::Result<Upgraded> {
    let tcp = TcpStream::connect(config.server_addr)
        .await
        .with_context(|| format!("failed to connect to {}", config.server_addr))?;
    tcp.set_nodelay(true)
        .context("failed to set TCP_NODELAY on the connection socket")?;

    match &config.cert_path {
        Some(cert_path) => {
            let connector = tls::CertBundle::read(cert_path)?.connector()?;
            let tls = connector
                .connect(tls::server_name(), tcp)
                .await
                .context("TLS handshake failed")?;
            upgrade(tls, protocol, muxer_config).await
        },
        None => upgrade(tcp, protocol, muxer_config).await,
    }
}

/// Sends the upgrade request over `io` and returns the upgraded connection.
async fn upgrade<IO>(io: IO, protocol: &str, muxer_config: Vec<u8>) -> anyhow::Result<Upgraded>
where
    IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (mut sender, connection) =
        hyper::client::conn::http1::handshake::<_, Empty<Bytes>>(TokioIo::new(io))
            .await
            .context("HTTP/1.1 handshake failed")?;
    // The connection has to be polled for the request to progress.
    // It resolves once the connection is handed over to use after the upgrade.
    let connection = tokio::spawn(connection.with_upgrades());

    let host = tls::server_name().to_str().into_owned();
    let request = Request::builder()
        .method(Method::GET)
        .uri("/echo")
        .header(HOST, host)
        .header(CONNECTION, "upgrade")
        .header(UPGRADE, protocol)
        .header(MuxerConfig::HEADER_NAME, muxer_config)
        .body(Empty::new())
        .context("failed to build the upgrade request")?;

    let response = sender
        .send_request(request)
        .await
        .context("upgrade request failed")?;
    if response.status() != StatusCode::SWITCHING_PROTOCOLS {
        let status = response.status();
        // The server explains a rejection in the body.
        let reason = response
            .into_body()
            .collect()
            .await
            .map(|body| String::from_utf8_lossy(&body.to_bytes()).into_owned())
            .unwrap_or_default();
        anyhow::bail!("server rejected the upgrade to {protocol}: {status} {reason}");
    }

    let upgraded = hyper::upgrade::on(response)
        .await
        .context("HTTP/1.1 upgrade failed")?;
    connection
        .await
        .context("HTTP/1.1 connection task panicked")?
        .context("HTTP/1.1 connection failed")?;
    Ok(upgraded)
}
