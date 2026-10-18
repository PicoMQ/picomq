use clap::ValueEnum;
use picomq_http::HttpProtocol;

use crate::target::{Listeners, Target};
use crate::{ds, kafka};

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum Suite {
    Ds,
    Kafka,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Outcome {
    pub case: String,
    pub passed: bool,
    pub detail: Option<String>,
}

impl Suite {
    pub fn name(self) -> &'static str {
        match self {
            Suite::Ds => "ds",
            Suite::Kafka => "kafka",
        }
    }

    pub fn listeners(self) -> Listeners {
        match self {
            Suite::Ds => Listeners {
                protocol: HttpProtocol::Ds,
                kafka: false,
            },
            Suite::Kafka => Listeners {
                protocol: HttpProtocol::Pico,
                kafka: true,
            },
        }
    }

    pub async fn run(self, target: &Target) -> anyhow::Result<Vec<Outcome>> {
        match self {
            Suite::Ds => ds::run(target).await,
            Suite::Kafka => kafka::run(target).await,
        }
    }
}
