use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

pub(crate) const BOUNDS_MICROS: [u64; 14] = [
    500, 1_000, 2_500, 5_000, 10_000, 25_000, 50_000, 100_000, 250_000, 500_000, 1_000_000,
    2_500_000, 5_000_000, 10_000_000,
];

#[derive(Default)]
pub(crate) struct Histogram {
    buckets: [AtomicU64; BOUNDS_MICROS.len() + 1],
    count: AtomicU64,
    sum_micros: AtomicU64,
}

impl Histogram {
    pub(crate) fn observe(&self, elapsed: Duration) {
        let micros = u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX);
        let bucket = BOUNDS_MICROS.partition_point(|bound| *bound < micros);

        self.buckets[bucket].fetch_add(1, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum_micros.fetch_add(micros, Ordering::Relaxed);
    }

    pub(crate) fn count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }

    pub(crate) fn sum_seconds(&self) -> f64 {
        self.sum_micros.load(Ordering::Relaxed) as f64 / 1_000_000.0
    }

    pub(crate) fn cumulative(&self) -> impl Iterator<Item = (Option<u64>, u64)> + '_ {
        let mut running = 0;
        self.buckets.iter().enumerate().map(move |(index, bucket)| {
            running += bucket.load(Ordering::Relaxed);
            (BOUNDS_MICROS.get(index).copied(), running)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn samples_land_in_the_first_bucket_that_holds_them() {
        let histogram = Histogram::default();
        for micros in [100, 500, 501, 20_000, 60_000_000] {
            histogram.observe(Duration::from_micros(micros));
        }

        let cumulative: Vec<(Option<u64>, u64)> = histogram.cumulative().collect();
        assert_eq!(cumulative[0], (Some(500), 2));
        assert_eq!(cumulative[1], (Some(1_000), 3));
        assert_eq!(cumulative[5], (Some(25_000), 4));
        assert_eq!(cumulative.last(), Some(&(None, 5)));
        assert_eq!(histogram.count(), 5);
        assert_eq!(histogram.sum_seconds(), 60.021101);
    }
}
