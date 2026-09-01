//! Support code shared by the benchmark binaries in this crate.
//!
//! The binaries themselves live in `src/bin`:
//!
//! - `mux_compare`: rammux vs yamux vs hyper/HTTP-2 on identical workloads
//! - `congestion_bench`: rammux alone, across emulated link conditions
//! - `emu_proxy`: puts an out-of-process contender on the same emulated link

pub mod emu;
pub mod rtt;
