use secrecy::{ExposeSecret, SecretString};

pub const REDACTED: &str = "[REDACTED]";

pub fn serialize_secret<S: serde::Serializer>(
    secret: &SecretString,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(secret.expose_secret())
}

pub fn serialize_optional_secret<S: serde::Serializer>(
    secret: &Option<SecretString>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    match secret {
        Some(secret) => serializer.serialize_some(secret.expose_secret()),
        None => serializer.serialize_none(),
    }
}

pub fn serialize_redacted<S: serde::Serializer>(
    _secret: &SecretString,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(REDACTED)
}

pub fn serialize_optional_redacted<S: serde::Serializer>(
    secret: &Option<SecretString>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    match secret {
        Some(_) => serializer.serialize_some(REDACTED),
        None => serializer.serialize_none(),
    }
}
