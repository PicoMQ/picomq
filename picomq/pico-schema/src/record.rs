use bytes::Bytes;

#[derive(Clone, Debug, Default)]
pub struct Record {
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
    pub timestamp_delta: i64,
}

impl Record {
    pub fn key(&self) -> Option<&Bytes> {
        self.key.as_ref()
    }

    pub fn value(&self) -> Option<&Bytes> {
        self.value.as_ref()
    }

    pub fn builder() -> RecordBuilder {
        RecordBuilder::default()
    }
}

#[derive(Clone, Debug, Default)]
pub struct RecordBuilder {
    key: Option<Bytes>,
    value: Option<Bytes>,
    timestamp_delta: i64,
}

impl RecordBuilder {
    pub fn key(mut self, key: Bytes) -> Self {
        self.key = Some(key);
        self
    }

    pub fn value(mut self, value: Bytes) -> Self {
        self.value = Some(value);
        self
    }

    pub fn timestamp_delta(mut self, timestamp_delta: i64) -> Self {
        self.timestamp_delta = timestamp_delta;
        self
    }

    pub fn build(self) -> Record {
        Record {
            key: self.key,
            value: self.value,
            timestamp_delta: self.timestamp_delta,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct Batch {
    pub base_timestamp: i64,
    pub records: Vec<Record>,
}

impl Batch {
    pub fn builder() -> BatchBuilder {
        BatchBuilder::default()
    }
}

#[derive(Clone, Debug, Default)]
pub struct BatchBuilder {
    base_timestamp: i64,
    records: Vec<Record>,
}

impl BatchBuilder {
    pub fn base_timestamp(mut self, base_timestamp: i64) -> Self {
        self.base_timestamp = base_timestamp;
        self
    }

    pub fn record(mut self, record: Record) -> Self {
        self.records.push(record);
        self
    }

    pub fn build(self) -> Batch {
        Batch {
            base_timestamp: self.base_timestamp,
            records: self.records,
        }
    }
}
