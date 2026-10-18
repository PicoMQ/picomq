mod clients;

use crate::suite::Outcome;
use crate::target::Target;

pub async fn run(target: &Target) -> anyhow::Result<Vec<Outcome>> {
    let url = target.url()?;

    clients::install().await?;
    let cases = clients::run(url).await?;

    Ok(cases
        .into_iter()
        .filter(|case| matches!(case.status.as_str(), "passed" | "failed"))
        .map(|case| Outcome {
            passed: case.status == "passed",
            detail: (!case.failures.is_empty()).then(|| case.failures.join("\n")),
            case: case.name,
        })
        .collect())
}
