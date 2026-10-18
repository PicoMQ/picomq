use std::path::Path;

use serde::Deserialize;
use tokio::process::Command;

use crate::suite::Outcome;

const DETAIL_LINES: usize = 8;

#[derive(Deserialize)]
struct Report {
    #[serde(rename = "testResults")]
    files: Vec<File>,
}

#[derive(Deserialize)]
struct File {
    #[serde(rename = "assertionResults")]
    cases: Vec<Case>,
}

#[derive(Deserialize)]
struct Case {
    #[serde(rename = "fullName")]
    name: String,
    status: String,
    #[serde(rename = "failureMessages", default)]
    failures: Vec<String>,
}

pub async fn run(url: &str) -> anyhow::Result<Vec<Outcome>> {
    let project = Path::new(env!("CARGO_MANIFEST_DIR")).join("clients/ds");

    let installed = Command::new("npm")
        .args(["ci", "--no-audit", "--no-fund", "--silent"])
        .current_dir(&project)
        .status()
        .await?;
    anyhow::ensure!(
        installed.success(),
        "npm ci failed in {}",
        project.display()
    );

    let report = tempfile::NamedTempFile::new()?;
    Command::new(project.join("node_modules/.bin/vitest"))
        .args(["run", "--silent", "--reporter=json"])
        .arg(format!("--outputFile.json={}", report.path().display()))
        .env("CONFORMANCE_TEST_URL", url)
        .current_dir(&project)
        .output()
        .await?;

    let report: Report = serde_json::from_slice(&std::fs::read(report.path())?)?;

    Ok(report
        .files
        .into_iter()
        .flat_map(|file| file.cases)
        .filter(|case| matches!(case.status.as_str(), "passed" | "failed"))
        .map(|case| Outcome {
            passed: case.status == "passed",
            detail: (!case.failures.is_empty()).then(|| {
                case.failures
                    .join("\n")
                    .lines()
                    .take(DETAIL_LINES)
                    .collect::<Vec<_>>()
                    .join("\n")
            }),
            case: case.name,
        })
        .collect())
}
