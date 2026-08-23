//! Stored access-token record and its length-prefixed binary encoding.
//!
//! The value under `auth/token/{id}` holds everything needed to authenticate
//! and authorize except the secret. Layout mirrors the explicit binary style
//! of the stream registry entry. There is no format-version byte: the product
//! is unreleased, so the shape can change in place.

use std::collections::BTreeSet;

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::scope::{
    Audience, Operation, OperationGroups, ReadWrite, ResourceMatcher, ResourceSet, Scope,
};
use crate::token::{Verifier, ID_MAX_LEN, ID_MIN_LEN, VERIFIER_LEN};
use crate::AuthError;

/// Server-side token record. Never contains the secret.
#[derive(Clone, PartialEq, Eq)]
pub struct TokenRecord {
    pub id: String,
    pub verifier: Verifier,
    pub scope: Scope,
    /// Creation time in unix milliseconds.
    pub created_at_ms: i64,
    /// Id of the issuing token. Empty for bootstrap.
    pub issued_by: String,
}

impl std::fmt::Debug for TokenRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TokenRecord")
            .field("id", &self.id)
            .field("verifier", &self.verifier)
            .field("scope", &self.scope)
            .field("created_at_ms", &self.created_at_ms)
            .field("issued_by", &self.issued_by)
            .finish()
    }
}

impl TokenRecord {
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        put_str(&mut buf, &self.id);
        buf.put_slice(self.verifier.as_bytes());
        encode_scope(&mut buf, &self.scope);
        buf.put_i64(self.created_at_ms);
        put_str(&mut buf, &self.issued_by);
        buf.freeze()
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, AuthError> {
        let mut buf = bytes;
        let id = get_str(&mut buf)?;
        validate_id(&id)?;
        let verifier = get_verifier(&mut buf)?;
        let scope = decode_scope(&mut buf)?;
        let created_at_ms = get_i64(&mut buf)?;
        let issued_by = get_str(&mut buf)?;
        if !buf.is_empty() {
            return Err(AuthError::Malformed);
        }
        Ok(Self {
            id,
            verifier,
            scope,
            created_at_ms,
            issued_by,
        })
    }
}

fn validate_id(id: &str) -> Result<(), AuthError> {
    if !(ID_MIN_LEN..=ID_MAX_LEN).contains(&id.len()) {
        return Err(AuthError::Malformed);
    }
    Ok(())
}

fn encode_scope(buf: &mut BytesMut, scope: &Scope) {
    encode_resource_set(buf, &scope.streams);
    encode_resource_set(buf, &scope.tokens);
    encode_groups(buf, &scope.groups);
    buf.put_i32(scope.ops.len() as i32);
    for op in &scope.ops {
        buf.put_u8(operation_tag(*op));
    }
    buf.put_i32(scope.audiences.len() as i32);
    for audience in &scope.audiences {
        buf.put_u8(audience_tag(*audience));
    }
    buf.put_u8(scope.auto_prefix_streams as u8);
    buf.put_u8(scope.expires_at_ms.is_some() as u8);
    buf.put_i64(scope.expires_at_ms.unwrap_or(0));
}

fn decode_scope(buf: &mut &[u8]) -> Result<Scope, AuthError> {
    let streams = decode_resource_set(buf)?;
    let tokens = decode_resource_set(buf)?;
    let groups = decode_groups(buf)?;
    let ops_len = get_i32(buf)?;
    if ops_len < 0 {
        return Err(AuthError::Malformed);
    }
    let mut ops = BTreeSet::new();
    for _ in 0..ops_len {
        ops.insert(operation_from_tag(get_u8(buf)?)?);
    }
    let audiences_len = get_i32(buf)?;
    if audiences_len < 0 {
        return Err(AuthError::Malformed);
    }
    let mut audiences = BTreeSet::new();
    for _ in 0..audiences_len {
        audiences.insert(audience_from_tag(get_u8(buf)?)?);
    }
    let auto_prefix_streams = get_u8(buf)? == 1;
    let expires_flag = get_u8(buf)? == 1;
    let expires_raw = get_i64(buf)?;
    let expires_at_ms = expires_flag.then_some(expires_raw);
    Ok(Scope {
        streams,
        tokens,
        groups,
        ops,
        audiences,
        auto_prefix_streams,
        expires_at_ms,
    })
}

fn encode_resource_set(buf: &mut BytesMut, set: &ResourceSet) {
    buf.put_i32(set.matchers().len() as i32);
    for matcher in set.matchers() {
        match matcher {
            ResourceMatcher::Exact(name) => {
                buf.put_u8(1);
                put_str(buf, name);
            }
            ResourceMatcher::Prefix(prefix) => {
                buf.put_u8(2);
                put_str(buf, prefix);
            }
        }
    }
}

fn decode_resource_set(buf: &mut &[u8]) -> Result<ResourceSet, AuthError> {
    let count = get_i32(buf)?;
    if count < 0 {
        return Err(AuthError::Malformed);
    }
    // Cap the pre-allocation so corrupt counts cannot force a huge alloc.
    // Same convention as the metadata command codec.
    let mut matchers = Vec::with_capacity((count as usize).min(1024));
    for _ in 0..count {
        match get_u8(buf)? {
            1 => matchers.push(ResourceMatcher::Exact(get_str(buf)?)),
            2 => matchers.push(ResourceMatcher::Prefix(get_str(buf)?)),
            _ => return Err(AuthError::Malformed),
        }
    }
    Ok(ResourceSet::any_of(matchers))
}

fn encode_groups(buf: &mut BytesMut, groups: &OperationGroups) {
    encode_rw(buf, groups.stream);
    encode_rw(buf, groups.tokens);
    encode_rw(buf, groups.admin);
}

fn decode_groups(buf: &mut &[u8]) -> Result<OperationGroups, AuthError> {
    Ok(OperationGroups {
        stream: decode_rw(buf)?,
        tokens: decode_rw(buf)?,
        admin: decode_rw(buf)?,
    })
}

fn encode_rw(buf: &mut BytesMut, rw: ReadWrite) {
    buf.put_u8(rw.read as u8);
    buf.put_u8(rw.write as u8);
}

fn decode_rw(buf: &mut &[u8]) -> Result<ReadWrite, AuthError> {
    Ok(ReadWrite {
        read: get_u8(buf)? == 1,
        write: get_u8(buf)? == 1,
    })
}

fn operation_tag(op: Operation) -> u8 {
    match op {
        Operation::Read => 1,
        Operation::Head => 2,
        Operation::List => 3,
        Operation::Create => 4,
        Operation::Append => 5,
        Operation::Trim => 6,
        Operation::Close => 7,
        Operation::Delete => 8,
        Operation::IssueToken => 9,
        Operation::RevokeToken => 10,
        Operation::ListTokens => 11,
        Operation::ClusterRead => 12,
        Operation::NodeRead => 13,
        Operation::StreamInspect => 14,
        Operation::TransferStream => 15,
        Operation::UpdateNodeSlots => 16,
    }
}

fn operation_from_tag(tag: u8) -> Result<Operation, AuthError> {
    Ok(match tag {
        1 => Operation::Read,
        2 => Operation::Head,
        3 => Operation::List,
        4 => Operation::Create,
        5 => Operation::Append,
        6 => Operation::Trim,
        7 => Operation::Close,
        8 => Operation::Delete,
        9 => Operation::IssueToken,
        10 => Operation::RevokeToken,
        11 => Operation::ListTokens,
        12 => Operation::ClusterRead,
        13 => Operation::NodeRead,
        14 => Operation::StreamInspect,
        15 => Operation::TransferStream,
        16 => Operation::UpdateNodeSlots,
        _ => return Err(AuthError::Malformed),
    })
}

fn audience_tag(audience: Audience) -> u8 {
    match audience {
        Audience::Pico => 1,
        Audience::DurableStreams => 2,
        Audience::Admin => 3,
    }
}

fn audience_from_tag(tag: u8) -> Result<Audience, AuthError> {
    Ok(match tag {
        1 => Audience::Pico,
        2 => Audience::DurableStreams,
        3 => Audience::Admin,
        _ => return Err(AuthError::Malformed),
    })
}

fn put_str(buf: &mut BytesMut, s: &str) {
    buf.put_i32(s.len() as i32);
    buf.put_slice(s.as_bytes());
}

fn get_u8(buf: &mut &[u8]) -> Result<u8, AuthError> {
    ensure(buf, 1)?;
    Ok(buf.get_u8())
}

fn get_i32(buf: &mut &[u8]) -> Result<i32, AuthError> {
    ensure(buf, 4)?;
    Ok(buf.get_i32())
}

fn get_i64(buf: &mut &[u8]) -> Result<i64, AuthError> {
    ensure(buf, 8)?;
    Ok(buf.get_i64())
}

fn get_str(buf: &mut &[u8]) -> Result<String, AuthError> {
    let len = get_i32(buf)?;
    if len < 0 {
        return Err(AuthError::Malformed);
    }
    ensure(buf, len as usize)?;
    let s = String::from_utf8(buf[..len as usize].to_vec()).map_err(|_| AuthError::Malformed)?;
    buf.advance(len as usize);
    Ok(s)
}

fn get_verifier(buf: &mut &[u8]) -> Result<Verifier, AuthError> {
    ensure(buf, VERIFIER_LEN)?;
    let mut bytes = [0u8; VERIFIER_LEN];
    bytes.copy_from_slice(&buf[..VERIFIER_LEN]);
    buf.advance(VERIFIER_LEN);
    Ok(Verifier::from_bytes(bytes))
}

fn ensure(buf: &[u8], n: usize) -> Result<(), AuthError> {
    if buf.remaining() < n {
        return Err(AuthError::Malformed);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::token::AccessToken;
    use proptest::prelude::*;

    fn sample_scope() -> Scope {
        Scope {
            streams: ResourceSet::any_of([
                ResourceMatcher::Prefix("/acct/".into()),
                ResourceMatcher::Exact("/shared".into()),
            ]),
            tokens: ResourceSet::prefix("acct/"),
            groups: OperationGroups {
                stream: ReadWrite::read_only(),
                ..OperationGroups::default()
            },
            ops: [Operation::IssueToken].into(),
            audiences: [Audience::Pico, Audience::Admin].into(),
            auto_prefix_streams: false,
            expires_at_ms: Some(1_700_000_000_000),
        }
    }

    fn sample_record() -> TokenRecord {
        let (_, verifier) = AccessToken::issue("acct/reader").unwrap();
        TokenRecord {
            id: "acct/reader".into(),
            verifier,
            scope: sample_scope(),
            created_at_ms: 1_600_000_000_000,
            issued_by: "root".into(),
        }
    }

    #[test]
    fn round_trip() {
        let record = sample_record();
        let encoded = record.encode();
        let decoded = TokenRecord::decode(&encoded).unwrap();
        assert_eq!(decoded, record);
    }

    #[test]
    fn empty_scope_round_trip() {
        let (_, verifier) = AccessToken::issue("x").unwrap();
        let record = TokenRecord {
            id: "x".into(),
            verifier,
            scope: Scope::default(),
            created_at_ms: 0,
            issued_by: String::new(),
        };
        assert_eq!(TokenRecord::decode(&record.encode()).unwrap(), record);
    }

    #[test]
    fn truncated_rejected() {
        let encoded = sample_record().encode();
        for len in 0..encoded.len() {
            assert!(
                TokenRecord::decode(&encoded[..len]).is_err(),
                "expected error at truncated length {len}"
            );
        }
    }

    #[test]
    fn trailing_bytes_rejected() {
        let mut bytes = sample_record().encode().to_vec();
        bytes.push(0);
        assert!(matches!(
            TokenRecord::decode(&bytes),
            Err(AuthError::Malformed)
        ));
    }

    #[test]
    fn unknown_operation_tag_rejected() {
        let (_, verifier) = AccessToken::issue("y").unwrap();
        let mut scope = Scope::default();
        scope.ops.insert(Operation::Read);
        let record = TokenRecord {
            id: "y".into(),
            verifier,
            scope,
            created_at_ms: 1,
            issued_by: String::new(),
        };
        let mut bytes = record.encode().to_vec();
        // Corrupt the single op tag (1 = Read) that follows the ops count.
        let pos = bytes
            .windows(5)
            .position(|w| w[0..4] == [0, 0, 0, 1] && w[4] == 1)
            .expect("ops count + Read tag");
        bytes[pos + 4] = 99;
        assert!(matches!(
            TokenRecord::decode(&bytes),
            Err(AuthError::Malformed)
        ));
    }

    // Generators mirroring codec.rs's proptest style: encoding correctness
    // must hold for arbitrary field values, not just the fixture above.
    fn arb_matcher() -> impl Strategy<Value = ResourceMatcher> {
        prop_oneof![
            "[a-z/]{0,24}".prop_map(ResourceMatcher::Exact),
            "[a-z/]{0,24}".prop_map(ResourceMatcher::Prefix),
        ]
    }

    fn arb_resource_set() -> impl Strategy<Value = ResourceSet> {
        proptest::collection::vec(arb_matcher(), 0..4).prop_map(ResourceSet::any_of)
    }

    fn arb_rw() -> impl Strategy<Value = ReadWrite> {
        (any::<bool>(), any::<bool>()).prop_map(|(read, write)| ReadWrite { read, write })
    }

    fn arb_scope() -> impl Strategy<Value = Scope> {
        (
            arb_resource_set(),
            arb_resource_set(),
            (arb_rw(), arb_rw(), arb_rw()),
            proptest::collection::btree_set(
                proptest::sample::select(Operation::ALL.to_vec()),
                0..6,
            ),
            proptest::collection::btree_set(
                proptest::sample::select(vec![
                    Audience::Pico,
                    Audience::DurableStreams,
                    Audience::Admin,
                ]),
                0..3,
            ),
            any::<bool>(),
            proptest::option::of(any::<i64>()),
        )
            .prop_map(
                |(streams, tokens, (stream, tokens_rw, admin), ops, audiences, auto, exp)| Scope {
                    streams,
                    tokens,
                    groups: OperationGroups {
                        stream,
                        tokens: tokens_rw,
                        admin,
                    },
                    ops,
                    audiences,
                    auto_prefix_streams: auto,
                    expires_at_ms: exp,
                },
            )
    }

    fn arb_record() -> impl Strategy<Value = TokenRecord> {
        (
            "[a-z0-9/_-]{1,96}",
            any::<[u8; VERIFIER_LEN]>(),
            arb_scope(),
            any::<i64>(),
            "[a-z/]{0,32}",
        )
            .prop_map(
                |(id, verifier, scope, created_at_ms, issued_by)| TokenRecord {
                    id,
                    verifier: Verifier::from_bytes(verifier),
                    scope,
                    created_at_ms,
                    issued_by,
                },
            )
    }

    proptest! {
        #[test]
        fn roundtrip_arbitrary_records(record in arb_record()) {
            let encoded = record.encode();
            prop_assert_eq!(&TokenRecord::decode(&encoded).unwrap(), &record);
        }

        #[test]
        fn arbitrary_prefixes_and_extensions_rejected(record in arb_record()) {
            let encoded = record.encode();
            for len in 0..encoded.len() {
                prop_assert!(TokenRecord::decode(&encoded[..len]).is_err());
            }
            let mut extended = encoded.to_vec();
            extended.push(0);
            prop_assert!(TokenRecord::decode(&extended).is_err());
        }

        #[test]
        fn arbitrary_bytes_never_panic(bytes in proptest::collection::vec(any::<u8>(), 0..256)) {
            let _ = TokenRecord::decode(&bytes);
        }
    }
}
