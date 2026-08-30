use bytes::Bytes;

#[derive(Clone, Debug, Default)]
pub struct Record {
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
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

    pub fn build(self) -> Record {
        Record {
            key: self.key,
            value: self.value,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct Batch {
    pub records: Vec<Record>,
}

impl Batch {
    pub fn builder() -> BatchBuilder {
        BatchBuilder::default()
    }
}

#[derive(Clone, Debug, Default)]
pub struct BatchBuilder {
    records: Vec<Record>,
}

impl BatchBuilder {
    pub fn record(mut self, record: Record) -> Self {
        self.records.push(record);
        self
    }

    pub fn build(self) -> Batch {
        Batch {
            records: self.records,
        }
    }
}
