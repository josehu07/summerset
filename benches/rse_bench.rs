//! Reed-Solomon erasure coding computation overhead benchmarking.
#![allow(clippy::uninlined_format_args)]

use std::collections::HashMap;
use std::sync::LazyLock;
use std::time::Duration;
use std::{fmt, hint};

use criterion::measurement::{Measurement, ValueFormatter, WallTime};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use rand::Rng;
use rand::distr::Alphanumeric;
use reed_solomon_erasure::galois_8::ReedSolomon;
use summerset::{RSCodeword, SummersetError};
use sysinfo::System;

// static SCHEMES: [(u8, u8); 4] = [(3, 2), (6, 4), (9, 6), (12, 8)];
static SCHEMES: [(u8, u8); 1] = [(3, 2)];
static SIZES: [usize; 6] = [
    4096,
    16 * 1024,
    64 * 1024,
    256 * 1024,
    1024 * 1024,
    4096 * 1024,
];

struct BenchId(usize, (u8, u8));

impl fmt::Display for BenchId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}@({},{})", self.0, self.1.0, self.1.1)
    }
}

/// A very long pre-generated value string to get values from.
static MOM_VALUE: LazyLock<String> = LazyLock::new(|| {
    rand::rng()
        .sample_iter(&Alphanumeric)
        .take(4096 * 1024)
        .map(char::from)
        .collect()
});

/// Reed-Solomon coder.
static RS_CODER: LazyLock<HashMap<(u8, u8), ReedSolomon>> =
    LazyLock::new(|| {
        SCHEMES
            .iter()
            .map(|&s| {
                (s, ReedSolomon::new(s.0 as usize, s.1 as usize).unwrap())
            })
            .collect()
    });

/// Placeholder formatter for criterion measurement.
struct GarbageFormatter;

impl ValueFormatter for GarbageFormatter {
    fn scale_values(
        &self,
        _typical_value: f64,
        _values: &mut [f64],
    ) -> &'static str {
        "N/A"
    }

    fn scale_throughputs(
        &self,
        _typical_value: f64,
        _throughput: &criterion::Throughput,
        _values: &mut [f64],
    ) -> &'static str {
        "N/A"
    }

    fn scale_for_machines(&self, _values: &mut [f64]) -> &'static str {
        "N/A"
    }
}

/// Custom measurement for CPU usage (percentage of total).
#[derive(Clone, Debug)]
struct CpuUsage;

impl Measurement for CpuUsage {
    type Intermediate = System; // refreshed once at start
    type Value = f64; // usage percent

    fn start(&self) -> Self::Intermediate {
        let mut sys = System::new_all();
        sys.refresh_cpu_all();
        sys
    }

    fn end(&self, mut sys: Self::Intermediate) -> Self::Value {
        sys.refresh_cpu_all();
        f64::from(sys.global_cpu_usage())
    }

    fn add(&self, v1: &Self::Value, v2: &Self::Value) -> Self::Value {
        v1 + v2
    }

    fn zero(&self) -> Self::Value {
        0.0
    }

    fn to_f64(&self, value: &Self::Value) -> f64 {
        let res = *value / 2.0; // average across start/end refresh window
        println!("  cpu: {:.3} %", res);
        if res > 0.0 { res } else { 0.001 }
    }

    fn formatter(&self) -> &dyn ValueFormatter {
        &GarbageFormatter
    }
}

/// Custom measurement for memory usage overhead (physical memory delta, bytes).
#[derive(Clone, Debug)]
struct MemUsage;

impl Measurement for MemUsage {
    type Intermediate = (System, u64); // system snapshot + used memory (bytes)
    type Value = u64;

    fn start(&self) -> Self::Intermediate {
        let mut sys = System::new_all();
        sys.refresh_memory();
        let used_bytes = sys.used_memory() * 1024; // sysinfo reports KiB
        (sys, used_bytes)
    }

    fn end(&self, (mut sys, used_start): Self::Intermediate) -> Self::Value {
        sys.refresh_memory();
        let used_end = sys.used_memory() * 1024;
        used_end.saturating_sub(used_start)
    }

    fn add(&self, v1: &Self::Value, v2: &Self::Value) -> Self::Value {
        v1 + v2
    }

    fn zero(&self) -> Self::Value {
        0
    }

    fn to_f64(&self, value: &Self::Value) -> f64 {
        #[allow(clippy::cast_precision_loss)]
        let res = *value as f64;
        println!("  mem: {:.0} B", res);
        if res > 0.0 { res } else { 1.0 }
    }

    fn formatter(&self) -> &dyn ValueFormatter {
        &GarbageFormatter
    }
}

fn compute_codeword(
    size: usize,
    scheme: (u8, u8),
) -> Result<(), SummersetError> {
    let value = MOM_VALUE[..size].to_string();
    let mut cw = RSCodeword::<String>::from_data(value, scheme.0, scheme.1)?;
    cw.compute_parity(Some(RS_CODER.get(&scheme).unwrap()))?;
    hint::black_box(Ok(()))
}

fn rse_bench_group<M: Measurement, const N: char>(c: &mut Criterion<M>) {
    let measurement_name = match N {
        't' => "time_taken",
        'c' => "cpu_usage",
        'm' => "mem_usage",
        _ => unreachable!(),
    };
    let mut group = c.benchmark_group(measurement_name);
    group
        .sample_size(50)
        .warm_up_time(Duration::from_millis(100))
        .measurement_time(Duration::from_secs(6));

    for size in SIZES {
        for scheme in SCHEMES {
            // manual print separator for easier parsing of outputs
            println!(
                "Benchmarking {}/{}@({},{}) EFFECTIVE STARTING",
                measurement_name, size, scheme.0, scheme.1
            );

            group.bench_with_input(
                BenchmarkId::from_parameter(BenchId(size, scheme)),
                &BenchId(size, scheme),
                |b, bench_id| {
                    b.iter(|| compute_codeword(bench_id.0, bench_id.1));
                },
            );
        }
    }

    group.finish();
}

criterion_group! {
    name = time_taken;
    config = Criterion::default().without_plots();
    targets = rse_bench_group<WallTime, 't'>,
}
criterion_group! {
    name = cpu_usage;
    config = Criterion::default().without_plots().with_measurement(CpuUsage);
    targets = rse_bench_group<CpuUsage, 'c'>
}
criterion_group! {
    name = mem_usage;
    config = Criterion::default().without_plots().with_measurement(MemUsage);
    targets = rse_bench_group<MemUsage, 'm'>
}

criterion_main!(time_taken, cpu_usage, mem_usage);
