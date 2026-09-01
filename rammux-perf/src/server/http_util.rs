use std::str::FromStr;

use anyhow::Context;
use hyper::HeaderMap;

pub fn prop_from_header<T>(headers: &HeaderMap, header_name: &str) -> anyhow::Result<T>
where
    T: FromStr,
    T::Err: Send + Sync + std::error::Error + 'static,
{
    headers
        .get(header_name)
        .context("header not found")
        .and_then(|value| value.to_str().context("header value is not a valid UTF-8"))
        .and_then(|value| value.parse().context("header value is invalid"))
        .with_context(|| format!("failed to extract config option from header `{header_name}`"))
}
