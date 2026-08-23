//! Auth failures. Hosts map these to protocol-specific HTTP responses.

use thiserror::Error;

/// Why authentication or authorization failed.
///
/// Unknown ids and bad secrets both use [`Unauthenticated`] so callers cannot
/// probe which token ids exist.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum AuthError {
    /// Missing header, unparseable bearer, unknown id, or secret mismatch.
    #[error("unauthenticated")]
    Unauthenticated,

    /// Bearer string did not match the expected wire form.
    #[error("malformed token")]
    Malformed,

    /// Token record is past its expiry.
    #[error("token expired")]
    Expired,

    /// Token is not valid for this listener or protocol.
    #[error("wrong audience")]
    WrongAudience,

    /// Authenticated, but the scope does not allow this operation or resource.
    #[error("permission denied")]
    Denied,

    /// Issue request would widen the issuer scope (rejected, not clamped).
    #[error("scope narrowing rejected")]
    NarrowingRejected,

    /// Backing store failed. Not a client credential problem.
    #[error("auth store: {0}")]
    Store(String),
}

impl AuthError {
    /// True when the caller should retry after fixing credentials or scope.
    /// Store failures are the only potentially transient class.
    pub fn is_client(&self) -> bool {
        !matches!(self, Self::Store(_))
    }
}
