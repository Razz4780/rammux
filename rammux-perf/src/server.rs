use std::{future::Ready, net::SocketAddr, path::Path};

use anyhow::Context;
use base64::Engine;
use futures::TryFutureExt;
use hyper::{
    HeaderMap, Method, Request, Response, StatusCode,
    body::Incoming,
    service::Service,
    upgrade::{OnUpgrade, Upgraded},
};
use hyper_util::rt::TokioIo;
use serde::de::DeserializeOwned;
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::TlsAcceptor;
use tracing::{Instrument, Level};

use crate::{
    config::{MuxerConfig, ServerConfig},
    server::{h2_echo::H2Echo, rammux_echo::RammuxEcho, yamux_echo::YamuxEcho},
    signal::ExitSignal,
    tls,
};

mod h2_echo;
mod pipe;
mod rammux_echo;
mod yamux_echo;

pub async fn run(config: &Path) -> anyhow::Result<()> {
    let config = {
        let raw = std::fs::read(config)
            .with_context(|| format!("failed to read config file at {}", config.display()))?;
        serde_json::from_slice::<ServerConfig>(&raw)
            .with_context(|| format!("failed to parse config file at {}", config.display()))?
    };
    let acceptor = tls::CertBundle::read(&config.cert_path)?.acceptor()?;
    let mut signal = ExitSignal::new()?;
    let http_listener = TcpListener::bind(config.http_addr)
        .await
        .with_context(|| format!("failed to bind on {}", config.http_addr))?;
    let https_listener = TcpListener::bind(config.https_addr)
        .await
        .with_context(|| format!("failed to bind on {}", config.https_addr))?;

    loop {
        let ((stream, peer), use_tls) = tokio::select! {
            () = &mut signal => {
                tracing::info!("Received an exit signal");
                break Ok(());
            },

            result = http_listener.accept() => {
                (
                    result.context("failed to accept a TCP connection")?,
                    false,
                )
            },

            result = https_listener.accept() => {
                (
                    result.context("failed to accept a TCP connection")?,
                    true,
                )
            },
        };
        stream
            .set_nodelay(true)
            .context("failed to set TCP_NODELAY on a TCP connection socket")?;
        stream
            .set_zero_linger()
            .context("failed to set 0 duration SO_LINGER on a TCP connection socket")?;
        tokio::spawn(handle_http_connection(
            stream,
            peer,
            use_tls.then(|| acceptor.clone()),
        ));
    }
}

#[tracing::instrument(
    level = Level::INFO,
    skip(stream, acceptor),
    fields(with_tls = acceptor.is_some()),
)]
async fn handle_http_connection(
    stream: TcpStream,
    peer: SocketAddr,
    acceptor: Option<TlsAcceptor>,
) {
    tracing::info!("Handling new HTTP/1.1 connection");

    let result = match acceptor {
        Some(acceptor) => {
            acceptor
                .accept(stream)
                .map_err(|error| anyhow::Error::new(error).context("TLS handshake failed"))
                .and_then(|stream| {
                    hyper::server::conn::http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), HttpUpgradeService)
                        .with_upgrades()
                        .map_err(|error| {
                            anyhow::Error::new(error).context("HTTP/1.1 service failed")
                        })
                })
                .await
        },
        None => hyper::server::conn::http1::Builder::new()
            .serve_connection(TokioIo::new(stream), HttpUpgradeService)
            .with_upgrades()
            .await
            .context("HTTP/1.1 connection failed"),
    };

    match result {
        Ok(()) => tracing::info!("HTTP/1.1 connection finished"),
        Err(error) => {
            tracing::warn!(error = format!("{error:#}"), "HTTP/1.1 connection failed",);
        },
    }
}

/// HTTP/1.1 [`Service`] that only serves UPGRADE requests to known multiplexing protocols:
/// * rammux
/// * yamux
/// * smux
/// * h2
///
/// Upgraded connection is served by one of [`EchoImpl`] implementations.
struct HttpUpgradeService;

impl HttpUpgradeService {
    fn build_and_start_echo<E: EchoImpl + Send + 'static>(
        upgrade: OnUpgrade,
        headers: &HeaderMap,
    ) -> anyhow::Result<()> {
        let config = std::panic::catch_unwind(|| {
            let config = headers
                .get(MuxerConfig::HEADER_NAME)
                .context("header not found")?
                .as_bytes();
            let config = base64::engine::general_purpose::URL_SAFE_NO_PAD
                .decode(config)
                .context("header is not valid base64")?;
            serde_json::from_slice(&config).context("failed to deserialize muxer config")
        })
        .map_err(|_| {
            anyhow::anyhow!(
                "failed to build {} config from headers - panic",
                E::protocol_name()
            )
        })?
        .with_context(|| {
            format!(
                "failed to deserialize {} config from {} header",
                E::protocol_name(),
                MuxerConfig::HEADER_NAME
            )
        })?;

        tokio::spawn(
            async move {
                let upgraded = match upgrade.await {
                    Ok(upgraded) => upgraded,
                    Err(error) => {
                        tracing::warn!(
                            %error,
                            "HTTP/1.1 upgrade failed",
                        );
                        return;
                    },
                };
                match E::run_on(upgraded, config).await {
                    Ok(()) => tracing::info!(
                        upgrade_protocol = E::protocol_name(),
                        "Upgraded HTTP/1.1 connection finished",
                    ),
                    Err(error) => {
                        tracing::warn!(
                            upgrade_protocol = E::protocol_name(),
                            error = format!("{error:#}"),
                            "Upgraded HTTP/1.1 connection failed",
                        );
                    },
                }
            }
            .in_current_span(),
        );

        Ok(())
    }

    fn call_inner(mut req: Request<Incoming>) -> anyhow::Result<Response<String>> {
        anyhow::ensure!(req.uri().path() == "/echo", "expected path /echo");
        anyhow::ensure!(req.method() == Method::GET, "expected method GET");
        anyhow::ensure!(
            req.headers()
                .get("connection")
                .is_some_and(|h| h.as_bytes() == b"upgrade"),
            "expected connection: upgrade header",
        );
        let upgrade = hyper::upgrade::on(&mut req);
        let protocol = req
            .headers()
            .get("upgrade")
            .and_then(|value| value.to_str().ok());

        match protocol {
            Some("rammux") => Self::build_and_start_echo::<RammuxEcho>(upgrade, req.headers())?,
            Some("yamux") => Self::build_and_start_echo::<YamuxEcho>(upgrade, req.headers())?,
            Some("h2") => Self::build_and_start_echo::<H2Echo>(upgrade, req.headers())?,
            Some(unknown) => anyhow::bail!("unknown upgrade protocol {unknown}"),
            None => anyhow::bail!("expecteed upgrade header"),
        };

        Ok(Response::builder()
            .status(StatusCode::SWITCHING_PROTOCOLS)
            .body(Default::default())
            .unwrap())
    }
}

impl Service<Request<Incoming>> for HttpUpgradeService {
    type Response = Response<String>;
    type Error = hyper::Error;
    type Future = Ready<Result<Self::Response, Self::Error>>;

    fn call(&self, req: Request<Incoming>) -> Self::Future {
        tracing::info!(
            uri = %req.uri(),
            method = %req.method(),
            upgrade_protocol = req.headers().get("upgrade").and_then(|v| v.to_str().ok()),
            "Received an HTTP/1.1 request",
        );
        let response = Self::call_inner(req).unwrap_or_else(|error| {
            let error = format!("{error:#}");
            tracing::warn!(error, "HTTP/1.1 request rejected",);
            Response::builder()
                .status(StatusCode::IM_A_TEAPOT) // whatever
                .body(error)
                .unwrap()
        });
        std::future::ready(Ok(response))
    }
}

/// Echo implementation with some multiplexing protocol.
trait EchoImpl: Sized {
    type Config: DeserializeOwned + Send + 'static;

    fn protocol_name() -> &'static str;

    fn run_on(
        conn: Upgraded,
        config: Self::Config,
    ) -> impl Future<Output = anyhow::Result<()>> + Send;
}
