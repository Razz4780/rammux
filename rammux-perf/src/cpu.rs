use std::time::Duration;

use anyhow::Context;

const USER_HZ: u64 = 100;

/// Returns process-wide CPU time so far (both user and sys).
///
/// Can be used to measure how CPU-heavy a multiplexing protocol is.
fn cpu_time() -> anyhow::Result<Duration> {
    let stat =
        std::fs::read_to_string("/proc/self/stat").context("failed to read /proc/self/stat")?;

    let mut fields = stat
        .rsplit_once(')')
        .context("unexpected /proc/self/stat output")?
        .1
        .split_whitespace()
        .skip(11);
    let utime: u64 = fields
        .next()
        .and_then(|field| field.parse::<u64>().ok())
        .context("unexpected /proc/self/stat output")?;
    let stime: u64 = fields
        .next()
        .and_then(|field| field.parse::<u64>().ok())
        .context("unexpected /proc/self/stat output")?;

    let total_ms = (utime + stime) * 1000 / USER_HZ;
    Ok(Duration::from_millis(total_ms))
}

#[cfg(test)]
mod test {
    use std::time::{Duration, Instant};

    #[test]
    fn cpu_time_works() {
        let before = super::cpu_time().unwrap();

        let start = Instant::now();
        while start.elapsed() < Duration::from_millis(200) {
            std::hint::black_box(());
        }

        let after = super::cpu_time().unwrap();
        assert!(after > before);
    }
}
