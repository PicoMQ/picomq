//! Opaque access-token wire form, secrets, and verifiers.
//!
//! Wire shape: `BASE64URL(id) . BASE64URL(secret)` (no pad). The id is
//! opaque utf-8 after decode. The secret is 32 random bytes. Only a hash of
//! the secret is stored server-side.

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use zeroize::{Zeroize, ZeroizeOnDrop};

use crate::AuthError;

/// Domain separation for the verifier hash. Not a wire version tag.
const VERIFY_DOMAIN: &[u8] = b"picomq-access-token";

pub const SECRET_LEN: usize = 32;
pub const VERIFIER_LEN: usize = 32;
pub const ID_MIN_LEN: usize = 1;
pub const ID_MAX_LEN: usize = 96;

/// Raw token secret. Zeroized on drop. Never logged.
#[derive(Clone, Zeroize, ZeroizeOnDrop)]
pub struct Secret([u8; SECRET_LEN]);

impl Secret {
    pub fn generate() -> Result<Self, AuthError> {
        let mut bytes = [0u8; SECRET_LEN];
        getrandom::getrandom(&mut bytes).map_err(|e| AuthError::Store(e.to_string()))?;
        Ok(Self(bytes))
    }

    pub fn from_bytes(bytes: [u8; SECRET_LEN]) -> Self {
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &[u8; SECRET_LEN] {
        &self.0
    }
}

impl std::fmt::Debug for Secret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Secret([redacted])")
    }
}

/// SHA-256(domain || secret). Safe to store and compare.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Verifier([u8; VERIFIER_LEN]);

impl Verifier {
    pub fn from_secret(secret: &Secret) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(VERIFY_DOMAIN);
        hasher.update(secret.as_bytes());
        let digest = hasher.finalize();
        let mut out = [0u8; VERIFIER_LEN];
        out.copy_from_slice(&digest);
        Self(out)
    }

    pub fn from_bytes(bytes: [u8; VERIFIER_LEN]) -> Self {
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &[u8; VERIFIER_LEN] {
        &self.0
    }

    /// Constant-time compare of this verifier to one derived from `secret`.
    pub fn matches_secret(&self, secret: &Secret) -> bool {
        let other = Self::from_secret(secret);
        self.0.ct_eq(&other.0).into()
    }
}

impl std::fmt::Debug for Verifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Verifier([redacted])")
    }
}

/// Parsed bearer credential: public id + secret.
#[derive(Clone)]
pub struct AccessToken {
    pub id: String,
    pub secret: Secret,
}

impl AccessToken {
    pub fn issue(id: impl Into<String>) -> Result<(Self, Verifier), AuthError> {
        let id = id.into();
        validate_id(&id)?;
        let secret = Secret::generate()?;
        let verifier = Verifier::from_secret(&secret);
        Ok((Self { id, secret }, verifier))
    }

    /// Parse `BASE64URL(id).BASE64URL(secret)`.
    pub fn parse(raw: &str) -> Result<Self, AuthError> {
        let raw = raw.trim();
        let (id_b64, secret_b64) = raw.split_once('.').ok_or(AuthError::Malformed)?;
        if id_b64.is_empty() || secret_b64.is_empty() || secret_b64.contains('.') {
            return Err(AuthError::Malformed);
        }

        let id_bytes = URL_SAFE_NO_PAD
            .decode(id_b64)
            .map_err(|_| AuthError::Malformed)?;
        let id = String::from_utf8(id_bytes).map_err(|_| AuthError::Malformed)?;
        validate_id(&id)?;

        let secret_bytes = URL_SAFE_NO_PAD
            .decode(secret_b64)
            .map_err(|_| AuthError::Malformed)?;
        if secret_bytes.len() != SECRET_LEN {
            return Err(AuthError::Malformed);
        }
        let mut secret = [0u8; SECRET_LEN];
        secret.copy_from_slice(&secret_bytes);

        Ok(Self {
            id,
            secret: Secret::from_bytes(secret),
        })
    }

    pub fn render(&self) -> String {
        let id_b64 = URL_SAFE_NO_PAD.encode(self.id.as_bytes());
        let secret_b64 = URL_SAFE_NO_PAD.encode(self.secret.as_bytes());
        format!("{id_b64}.{secret_b64}")
    }
}

impl std::fmt::Debug for AccessToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AccessToken")
            .field("id", &self.id)
            .field("secret", &self.secret)
            .finish()
    }
}

fn validate_id(id: &str) -> Result<(), AuthError> {
    if !(ID_MIN_LEN..=ID_MAX_LEN).contains(&id.len()) {
        return Err(AuthError::Malformed);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn issue_round_trip_and_verify() {
        let (token, verifier) = AccessToken::issue("svc/ingest").unwrap();
        assert_eq!(token.id, "svc/ingest");
        assert!(verifier.matches_secret(&token.secret));

        let wire = token.render();
        let parsed = AccessToken::parse(&wire).unwrap();
        assert_eq!(parsed.id, "svc/ingest");
        assert!(verifier.matches_secret(&parsed.secret));
    }

    #[test]
    fn wrong_secret_does_not_match() {
        let (_, verifier) = AccessToken::issue("a").unwrap();
        let other = Secret::generate().unwrap();
        assert!(!verifier.matches_secret(&other));
    }

    #[test]
    fn malformed_wires_rejected() {
        assert!(matches!(AccessToken::parse(""), Err(AuthError::Malformed)));
        assert!(matches!(
            AccessToken::parse("nodot"),
            Err(AuthError::Malformed)
        ));
        assert!(matches!(
            AccessToken::parse("a.b.c"),
            Err(AuthError::Malformed)
        ));
        assert!(matches!(AccessToken::issue(""), Err(AuthError::Malformed)));
    }

    #[test]
    fn secret_debug_is_redacted() {
        let secret = Secret::generate().unwrap();
        assert_eq!(format!("{secret:?}"), "Secret([redacted])");
    }

    proptest::proptest! {
        #[test]
        fn roundtrip_arbitrary_ids(id in "[a-z0-9/:_.-]{1,96}") {
            let (token, verifier) = AccessToken::issue(id.as_str()).unwrap();
            let parsed = AccessToken::parse(&token.render()).unwrap();
            proptest::prop_assert_eq!(&parsed.id, &id);
            proptest::prop_assert!(verifier.matches_secret(&parsed.secret));
        }

        #[test]
        fn tampered_secret_never_verifies(flip_at in 0usize..SECRET_LEN, delta in 1u8..=255) {
            let (token, verifier) = AccessToken::issue("t").unwrap();
            let mut bytes = *token.secret.as_bytes();
            bytes[flip_at] ^= delta;
            let tampered = Secret::from_bytes(bytes);
            proptest::prop_assert!(!verifier.matches_secret(&tampered));
        }
    }
}
