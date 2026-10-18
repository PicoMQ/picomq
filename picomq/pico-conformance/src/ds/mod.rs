mod clients;

use crate::suite::Outcome;
use crate::target::Target;

pub async fn run(target: &Target) -> anyhow::Result<Vec<Outcome>> {
    clients::run(target.url()?).await
}
