use crate::topic::TopicId;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Partition {
    pub topic: TopicId,
    pub index: u32,
}

impl Partition {
    pub fn new(topic: TopicId, index: u32) -> Self {
        Self { topic, index }
    }
}
