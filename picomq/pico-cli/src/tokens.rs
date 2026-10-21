//! `pico admin tokens`: list, issue, and revoke access tokens through the
//! admin listener. Scopes are built with the same types and JSON codec the
//! server decodes, so a flag the CLI accepts is a scope the server understands.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use clap::{Args, Subcommand};
use picomq_auth::json::{audience_from_name, operation_from_name, operation_name};
use picomq_auth::{
    Operation, ReadWrite, ResourceMatcher, ResourceSet, Scope, scope_from_json, scope_to_json,
};
use serde_json::Value;

use crate::admin::{Api, print_json};
use crate::io::note;

#[derive(Debug, Subcommand)]
pub enum TokenCommand {
    /// List the tokens visible to the caller. Secrets are never shown.
    List,
    /// Issue a token narrowed from the caller's scope. Prints the secret, once.
    Issue(Box<IssueArgs>),
    /// Revoke a token. Effective on the next request, cluster-wide.
    Revoke { id: String },
}

#[derive(Debug, Args)]
pub struct IssueArgs {
    /// Token id, e.g. `svc/ingest`. Public, and unique per cluster.
    pub id: String,

    /// Allow streams under this prefix. Repeatable.
    #[arg(long = "stream-prefix", value_name = "PREFIX")]
    pub stream_prefixes: Vec<String>,

    /// Allow this exact stream. Repeatable.
    #[arg(long = "stream", value_name = "NAME")]
    pub streams: Vec<String>,

    /// Allow managing token ids under this prefix. Repeatable.
    #[arg(long = "token-prefix", value_name = "PREFIX")]
    pub token_prefixes: Vec<String>,

    /// Allow managing this exact token id. Repeatable.
    #[arg(long = "token-id", value_name = "ID")]
    pub token_ids: Vec<String>,

    /// Grant an operation group as `GROUP:ACCESS`, where GROUP is `stream`,
    /// `tokens`, or `admin` and ACCESS is `read`, `write`, or `rw`. Repeatable.
    #[arg(long = "grant", value_name = "GROUP:ACCESS", value_parser = parse_grant)]
    pub grants: Vec<Grant>,

    /// Grant a single operation, such as `append` or `issue_token`. Repeatable.
    #[arg(long = "op", value_name = "OP", value_parser = operation_names())]
    pub ops: Vec<String>,

    /// Listener the token is valid on. Repeatable, at least one required.
    #[arg(
        long = "audience",
        value_name = "AUDIENCE",
        value_parser = ["pico", "durable_streams", "admin"]
    )]
    pub audiences: Vec<String>,

    /// Make stream names relative to the token's single stream prefix.
    #[arg(long)]
    pub auto_prefix: bool,

    /// Expire the token after this long, e.g. `90s`, `30m`, `12h`, `30d`.
    #[arg(long, value_name = "DURATION", value_parser = parse_duration)]
    pub expires_in: Option<Duration>,

    /// The full scope as JSON, as the admin API takes it. Replaces the flags.
    #[arg(
        long,
        value_name = "JSON",
        conflicts_with_all = [
            "stream_prefixes", "streams", "token_prefixes", "token_ids", "grants",
            "ops", "audiences", "auto_prefix", "expires_in",
        ]
    )]
    pub scope: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Group {
    Stream,
    Tokens,
    Admin,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Grant {
    group: Group,
    access: ReadWrite,
}

pub async fn run(api: &Api, json: bool, command: TokenCommand) -> Result<i32, String> {
    match command {
        TokenCommand::List => {
            let body = api.get("/admin/tokens").await?;
            if json {
                return print_json(&body);
            }
            for token in body["tokens"].as_array().into_iter().flatten() {
                println!("{}", describe(token)?);
            }
        }
        TokenCommand::Issue(args) => {
            let id = args.id.clone();
            let scope = build_scope(*args)?;
            let body = api
                .post(
                    "/admin/tokens",
                    &serde_json::json!({ "id": id, "scope": scope_to_json(&scope) }),
                )
                .await?;
            if json {
                return print_json(&body);
            }
            note(format!(
                "issued {} (the secret below is shown once)",
                describe(&body)?
            ));
            println!("{}", body["token"].as_str().unwrap_or_default());
        }
        TokenCommand::Revoke { id } => {
            api.delete(&token_path(&id)?).await?;
            note(format!("revoked id={id}"));
        }
    }
    Ok(0)
}

/// Ids may hold any character. Encode each segment and keep the slashes,
/// which the route's wildcard captures.
fn token_path(id: &str) -> Result<String, String> {
    let mut url = reqwest::Url::parse("http://admin/admin/tokens").map_err(|e| e.to_string())?;
    url.path_segments_mut()
        .map_err(|()| "cannot build the token path".to_owned())?
        .extend(id.split('/'));
    Ok(url.path().to_owned())
}

fn build_scope(args: IssueArgs) -> Result<Scope, String> {
    if let Some(raw) = args.scope {
        let value: Value = serde_json::from_str(&raw).map_err(|e| format!("--scope: {e}"))?;
        return scope_from_json(&value).map_err(|_| "--scope: invalid scope".to_owned());
    }
    let mut scope = Scope {
        streams: matchers(args.streams, args.stream_prefixes),
        tokens: matchers(args.token_ids, args.token_prefixes),
        auto_prefix_streams: args.auto_prefix,
        ..Scope::default()
    };
    for grant in args.grants {
        let group = match grant.group {
            Group::Stream => &mut scope.groups.stream,
            Group::Tokens => &mut scope.groups.tokens,
            Group::Admin => &mut scope.groups.admin,
        };
        group.read |= grant.access.read;
        group.write |= grant.access.write;
    }
    for name in args.ops {
        let op = operation_from_name(&name).ok_or_else(|| format!("unknown operation {name}"))?;
        scope.ops.insert(op);
    }
    for name in args.audiences {
        let audience =
            audience_from_name(&name).ok_or_else(|| format!("unknown audience {name}"))?;
        scope.audiences.insert(audience);
    }
    if scope.audiences.is_empty() {
        return Err("at least one --audience is required (pico, durable_streams, admin)".into());
    }
    if let Some(after) = args.expires_in {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| e.to_string())?;
        let at = now + after;
        scope.expires_at_ms = Some(i64::try_from(at.as_millis()).map_err(|e| e.to_string())?);
    }
    Ok(scope)
}

fn matchers(exact: Vec<String>, prefixes: Vec<String>) -> ResourceSet {
    ResourceSet::any_of(
        exact
            .into_iter()
            .map(ResourceMatcher::Exact)
            .chain(prefixes.into_iter().map(ResourceMatcher::Prefix)),
    )
}

/// One line per token in the shape of the other admin commands.
fn describe(token: &Value) -> Result<String, String> {
    let scope = scope_from_json(&token["scope"])
        .map_err(|_| format!("server returned an unreadable scope: {token}"))?;
    let rendered = scope_to_json(&scope);
    let list = |key: &str| {
        let items: Vec<String> = rendered[key]
            .as_array()
            .into_iter()
            .flatten()
            .map(|item| match item {
                Value::String(s) => s.clone(),
                Value::Object(m) => m
                    .iter()
                    .map(|(kind, v)| format!("{kind}:{}", v.as_str().unwrap_or_default()))
                    .collect(),
                other => other.to_string(),
            })
            .collect();
        if items.is_empty() {
            "-".to_owned()
        } else {
            items.join(",")
        }
    };
    let groups: Vec<String> = [
        ("stream", scope.groups.stream),
        ("tokens", scope.groups.tokens),
        ("admin", scope.groups.admin),
    ]
    .into_iter()
    .filter_map(|(name, rw)| match (rw.read, rw.write) {
        (true, true) => Some(format!("{name}:rw")),
        (true, false) => Some(format!("{name}:read")),
        (false, true) => Some(format!("{name}:write")),
        (false, false) => None,
    })
    .collect();
    let mut line = format!(
        "id={} streams={} tokens={} groups={} ops={} audiences={} expiresAtMs={}",
        token["id"].as_str().unwrap_or("-"),
        list("streams"),
        list("tokens"),
        if groups.is_empty() {
            "-".to_owned()
        } else {
            groups.join(",")
        },
        list("ops"),
        list("audiences"),
        scope
            .expires_at_ms
            .map_or_else(|| "never".to_owned(), |ms| ms.to_string()),
    );
    if scope.auto_prefix_streams {
        line.push_str(" autoPrefix=true");
    }
    if let Some(issuer) = token["issuedBy"].as_str().filter(|s| !s.is_empty()) {
        line.push_str(&format!(" issuedBy={issuer}"));
    }
    Ok(line)
}

fn operation_names() -> clap::builder::PossibleValuesParser {
    Operation::ALL.map(operation_name).into()
}

fn parse_grant(raw: &str) -> Result<Grant, String> {
    let (group, access) = raw
        .split_once(':')
        .ok_or_else(|| format!("expected GROUP:ACCESS, got {raw}"))?;
    let group = match group {
        "stream" => Group::Stream,
        "tokens" => Group::Tokens,
        "admin" => Group::Admin,
        other => return Err(format!("unknown group {other} (stream, tokens, admin)")),
    };
    let access = match access {
        "read" => ReadWrite::read_only(),
        "write" => ReadWrite {
            read: false,
            write: true,
        },
        "rw" => ReadWrite::all(),
        other => return Err(format!("unknown access {other} (read, write, rw)")),
    };
    Ok(Grant { group, access })
}

fn parse_duration(raw: &str) -> Result<Duration, String> {
    let split = raw
        .find(|c: char| !c.is_ascii_digit())
        .ok_or_else(|| format!("{raw}: missing unit (s, m, h, d)"))?;
    let (number, unit) = raw.split_at(split);
    let number: u64 = number
        .parse()
        .map_err(|_| format!("{raw}: expected a number followed by s, m, h, or d"))?;
    let multiplier = match unit {
        "s" => 1,
        "m" => 60,
        "h" => 3_600,
        "d" => 86_400,
        other => return Err(format!("{raw}: unknown unit {other} (s, m, h, d)")),
    };
    let seconds = number
        .checked_mul(multiplier)
        .ok_or_else(|| format!("{raw}: too long"))?;
    Ok(Duration::from_secs(seconds))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grants_parse_groups_and_access() {
        assert_eq!(
            parse_grant("stream:rw").unwrap(),
            Grant {
                group: Group::Stream,
                access: ReadWrite::all()
            }
        );
        assert_eq!(
            parse_grant("admin:read").unwrap().access,
            ReadWrite::read_only()
        );
        assert!(parse_grant("stream").is_err());
        assert!(parse_grant("streams:rw").is_err());
        assert!(parse_grant("stream:all").is_err());
    }

    #[test]
    fn token_paths_encode_segments_but_keep_slashes() {
        assert_eq!(
            token_path("svc/ingest").unwrap(),
            "/admin/tokens/svc/ingest"
        );
        assert_eq!(
            token_path("svc/a b?c#d").unwrap(),
            "/admin/tokens/svc/a%20b%3Fc%23d"
        );
    }

    #[test]
    fn durations_need_a_unit() {
        assert_eq!(parse_duration("90s").unwrap(), Duration::from_secs(90));
        assert_eq!(parse_duration("12h").unwrap(), Duration::from_secs(43_200));
        assert_eq!(
            parse_duration("30d").unwrap(),
            Duration::from_secs(2_592_000)
        );
        assert!(parse_duration("30").is_err());
        assert!(parse_duration("d").is_err());
        assert!(parse_duration("3w").is_err());
    }
}
