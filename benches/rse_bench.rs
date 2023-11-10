//! Reed-Solomon erasure coding computation overhead benchmarking.

use std::fmt;
use std::collections::HashMap;
use std::time::Duration;

use summerset::{SummersetError, RSCodeword};

use rand::Rng;
use rand::distributions::Alphanumeric;

use reed_solomon_erasure::galois_8::ReedSolomon;

use criterion::{
    black_box, criterion_group, criterion_main, BenchmarkId, Criterion,
};

use lazy_static::lazy_static;

static SCHEMES: [(u8, u8); 4] = [(3, 2), (6, 4), (9, 6), (12, 8)];
static SIZES: [usize; 6] = [
    4096,
    16 * 1024,
    64 * 1024,
    256 * 1024,
    1024 * 1024,
    4096 * 1024,
];

struct BenchId(pub usize, pub (u8, u8));

impl fmt::Display for BenchId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}@({},{})", self.0, self.1 .0, self.1 .1)
    }
}

lazy_static!(
    /// A very long pre-generated value string to get values from.
    static ref MOM_VALUE: String = rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(4096 * 1024)
        .map(char::from)
        .collect();

    /// Reed-Solomon coder.
    static ref RS_CODER: HashMap<(u8, u8), ReedSolomon> = SCHEMES
        .iter()
        .map(|&s| (s, ReedSolomon::new(s.0 as usize, s.1 as usize).unwrap()))
        .collect();
);

fn compute_codeword(
    size: usize,
    scheme: (u8, u8),
) -> Result<(), SummersetError> {
    let value = MOM_VALUE[..size].to_string();
    let mut cw = RSCodeword::<String>::from_data(value, scheme.0, scheme.1)?;
    cw.compute_parity(Some(RS_CODER.get(&scheme).unwrap()))?;
    black_box(Ok(()))
}

fn rse_bench_group(c: &mut Criterion) {
    let mut group = c.benchmark_group("rse_bench");
    group
        .sample_size(50)
        .warm_up_time(Duration::from_millis(100))
        .measurement_time(Duration::from_secs(6));

    for size in SIZES {
        for scheme in SCHEMES {
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

criterion_group!(benches, rse_bench_group);
criterion_main!(benches);
