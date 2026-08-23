//! Scoped opaque access tokens for PicoMQ.
//!
//! This crate is pure logic: token wire form, verification, scopes, narrowing,
//! and an authorizer over a pluggable store. It has no HTTP and no metadata
//! backend. Host crates wire it into the server and frontends.

#![forbid(unsafe_code)]

pub mod authorizer;
pub mod error;
pub mod json;
pub mod narrow;
pub mod record;
pub mod scope;
pub mod store;
pub mod token;

pub use authorizer::{AuthPrincipal, Authorizer, ANONYMOUS_TOKEN_ID};
pub use error::AuthError;
pub use json::{scope_from_json, scope_to_json};
pub use narrow::check_issue;
pub use record::TokenRecord;
pub use scope::{
    Audience, Operation, OperationGroups, ReadWrite, ResourceMatcher, ResourceSet, Scope,
};
pub use store::{MemoryTokenStore, TokenStore};
pub use token::{AccessToken, Secret, Verifier, ID_MAX_LEN, ID_MIN_LEN, SECRET_LEN, VERIFIER_LEN};
