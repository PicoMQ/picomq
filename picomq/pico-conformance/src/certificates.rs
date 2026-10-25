use std::path::{Path, PathBuf};

use picomq_listener::testing::Authority;
use tempfile::TempDir;

use crate::kafka::reference;

pub struct Certificates {
    dir: TempDir,
}

impl Certificates {
    pub fn mint() -> anyhow::Result<Self> {
        let authority = Authority::new()?;
        let reference = reference::host();
        let issued = authority.issue(&["localhost", "host.docker.internal", &reference])?;

        let certificates = Self {
            dir: tempfile::tempdir()?,
        };
        std::fs::write(certificates.ca(), authority.root_pem())?;
        issued.write(certificates.dir())?;
        std::fs::write(
            certificates.keystore(),
            format!("{}{}", issued.key, issued.certificate),
        )?;

        Ok(certificates)
    }

    pub fn dir(&self) -> &Path {
        self.dir.path()
    }

    pub fn ca(&self) -> PathBuf {
        self.dir().join("ca.pem")
    }

    pub fn certificate(&self) -> PathBuf {
        self.dir().join("certificate.pem")
    }

    pub fn key(&self) -> PathBuf {
        self.dir().join("key.pem")
    }

    pub fn keystore(&self) -> PathBuf {
        self.dir().join("keystore.pem")
    }
}
