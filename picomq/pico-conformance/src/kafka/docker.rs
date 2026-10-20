use std::process::{Output, Stdio};

use tokio::io::AsyncWriteExt;
use tokio::process::Command;

pub async fn output(args: &[&str], input: Option<&str>) -> anyhow::Result<Output> {
    let mut child = Command::new("docker")
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()?;

    if let (Some(mut stdin), Some(input)) = (child.stdin.take(), input) {
        stdin.write_all(input.as_bytes()).await?;
    }

    Ok(child.wait_with_output().await?)
}

pub async fn docker(args: &[&str]) -> anyhow::Result<String> {
    let output = output(args, None).await?;

    anyhow::ensure!(
        output.status.success(),
        "docker {} failed: {}",
        args.join(" "),
        String::from_utf8_lossy(&output.stderr)
    );

    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

pub fn docker_unchecked(args: &[&str]) -> String {
    std::process::Command::new("docker")
        .args(args)
        .output()
        .map(|output| String::from_utf8_lossy(&output.stdout).into_owned())
        .unwrap_or_default()
}
