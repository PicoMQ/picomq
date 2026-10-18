use anyhow::Context;
use serde::Deserialize;
use tokio::process::Command;

const PROJECT: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/clients/ds");

#[derive(Deserialize)]
struct VitestReport {
    #[serde(rename = "numFailedTests")]
    failed: usize,
    #[serde(rename = "testResults")]
    files: Vec<File>,
}

#[derive(Deserialize)]
struct File {
    #[serde(rename = "assertionResults")]
    cases: Vec<Case>,
}

#[derive(Deserialize)]
pub struct Case {
    #[serde(rename = "fullName")]
    pub name: String,
    pub status: String,
    #[serde(rename = "failureMessages", default)]
    pub failures: Vec<String>,
}

pub async fn install() -> anyhow::Result<()> {
    let status = Command::new("npm")
        .args(["ci", "--no-audit", "--no-fund", "--silent"])
        .current_dir(PROJECT)
        .status()
        .await?;

    anyhow::ensure!(status.success(), "npm ci failed in {PROJECT}");

    Ok(())
}

pub async fn run(url: &str) -> anyhow::Result<Vec<Case>> {
    let file = tempfile::NamedTempFile::new()?;

    let output = Command::new(format!("{PROJECT}/node_modules/.bin/vitest"))
        .args(["run", "--silent", "--reporter=json"])
        .arg(format!("--outputFile.json={}", file.path().display()))
        .env("CONFORMANCE_TEST_URL", url)
        .current_dir(PROJECT)
        .output()
        .await?;

    let failed = || {
        format!(
            "vitest failed ({}):\n{}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        )
    };

    let report: VitestReport =
        serde_json::from_slice(&std::fs::read(file.path())?).with_context(failed)?;

    anyhow::ensure!(output.status.success() || report.failed > 0, failed());

    Ok(report
        .files
        .into_iter()
        .flat_map(|file| file.cases)
        .collect())
}
