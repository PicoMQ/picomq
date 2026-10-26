mod histogram;
mod instruments;
mod text;

use instruments::{INSTRUMENTS, Instruments};

pub use instruments::{
    GaugeGuard, ListenerKind, Method, Reason, TransferOutcome, auth_failure, connection,
    coordinated_group, http, kafka, live_reader, name_kafka_operation, read, rebalance, transfer,
    written,
};

#[derive(Clone, Copy)]
pub struct Metrics {
    instruments: &'static Instruments,
}

impl std::fmt::Debug for Metrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Metrics").finish_non_exhaustive()
    }
}

impl Metrics {
    pub fn install() -> Self {
        Self {
            instruments: INSTRUMENTS.get_or_init(Instruments::new),
        }
    }

    pub fn render(&self, logs_owned: u64) -> String {
        text::render(self.instruments, logs_owned)
    }
}
