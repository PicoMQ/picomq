use std::process::Stdio;

use tokio::process::Command;

pub async fn docker(args: &[&str]) -> anyhow::Result<()> {
    let output = Command::new("docker").args(args).output().await?;

    anyhow::ensure!(
        output.status.success(),
        "docker {} failed: {}",
        args.join(" "),
        String::from_utf8_lossy(&output.stderr)
    );

    Ok(())
}

pub fn remove(args: &[&str]) {
    let _ = std::process::Command::new("docker")
        .args(args)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
}
