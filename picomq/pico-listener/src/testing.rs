use std::path::Path;

use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
};
use rustls::pki_types::CertificateDer;

use crate::tls::TlsConfig;

pub struct Authority {
    issuer: Issuer<'static, KeyPair>,
    root: CertificateDer<'static>,
    root_pem: String,
}

pub struct Issued {
    pub certificate: String,
    pub key: String,
    pub der: CertificateDer<'static>,
}

impl Authority {
    pub fn new() -> Result<Self, rcgen::Error> {
        let mut params = CertificateParams::new(Vec::<String>::new())?;
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params
            .distinguished_name
            .push(DnType::CommonName, "PicoMQ test CA");
        let key = KeyPair::generate()?;
        let root = params.self_signed(&key)?;

        Ok(Self {
            root: root.der().clone(),
            root_pem: root.pem(),
            issuer: Issuer::new(params, key),
        })
    }

    pub fn root(&self) -> &CertificateDer<'static> {
        &self.root
    }

    pub fn root_pem(&self) -> &str {
        &self.root_pem
    }

    pub fn issue(&self, names: &[&str]) -> Result<Issued, rcgen::Error> {
        let mut params = CertificateParams::new(
            names
                .iter()
                .map(|name| (*name).to_owned())
                .collect::<Vec<_>>(),
        )?;
        params
            .distinguished_name
            .push(DnType::CommonName, "PicoMQ test");
        params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        let key = KeyPair::generate()?;
        let certificate = params.signed_by(&key, &self.issuer)?;

        Ok(Issued {
            certificate: certificate.pem(),
            key: key.serialize_pem(),
            der: certificate.der().clone(),
        })
    }
}

impl Issued {
    pub fn write(&self, dir: &Path) -> std::io::Result<TlsConfig> {
        std::fs::write(dir.join("certificate.pem"), &self.certificate)?;
        std::fs::write(dir.join("key.pem"), &self.key)?;

        Ok(TlsConfig::new(
            dir.join("certificate.pem"),
            dir.join("key.pem"),
        ))
    }
}
