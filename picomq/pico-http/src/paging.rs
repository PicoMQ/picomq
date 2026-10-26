use axum::http::{Uri, header};
use axum::response::{IntoResponse, Response};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use picomq_server::Page;
use serde_json::{Map, Value, json};

use crate::http::query_param;

const DEFAULT_LIMIT: usize = 100;
const MAX_LIMIT: usize = 1_000;
const MAX_BYTES: usize = 1 << 20;

#[derive(Debug)]
pub(crate) struct Paging {
    pub(crate) limit: usize,
    pub(crate) after: Option<String>,
    filters: Map<String, Value>,
}

impl Paging {
    pub(crate) fn from_query(uri: &Uri, filters: &[&str]) -> Result<Self, String> {
        let limit = match query_param(uri, "limit").map_err(|error| error.to_string())? {
            None => DEFAULT_LIMIT,
            Some(raw) => raw
                .parse::<usize>()
                .ok()
                .filter(|limit| (1..=MAX_LIMIT).contains(limit))
                .ok_or_else(|| format!("limit must be between 1 and {MAX_LIMIT}"))?,
        };

        let mut active = Map::new();
        for name in filters {
            if let Some(value) = query_param(uri, name).map_err(|error| error.to_string())? {
                active.insert((*name).to_owned(), Value::String(value));
            }
        }

        let after = match query_param(uri, "cursor").map_err(|error| error.to_string())? {
            None => None,
            Some(raw) => Some(resume(&raw, &active)?),
        };

        Ok(Self {
            limit,
            after,
            filters: active,
        })
    }

    pub(crate) fn fetch_count(&self) -> usize {
        picomq_server::page::fetch_count(self.limit)
    }

    pub(crate) fn filter(&self, name: &str) -> Option<&str> {
        self.filters.get(name).and_then(Value::as_str)
    }

    pub(crate) fn reply<K: Clone + ToString, T>(
        &self,
        mut page: Page<K, T>,
        render: impl Fn(&K, &T) -> Value,
    ) -> Response {
        let mut items = Vec::new();
        let mut sent = 0;

        for (key, item) in &page.items {
            let encoded =
                serde_json::to_vec(&render(key, item)).expect("a JSON value always encodes");
            if sent > 0 && items.len() + encoded.len() > MAX_BYTES {
                break;
            }
            if sent > 0 {
                items.push(b',');
            }
            items.extend_from_slice(&encoded);
            sent += 1;
        }

        page.truncate(sent);
        let next = page.next.map(|after| self.cursor(&after.to_string()));

        let mut body = Vec::with_capacity(items.len() + 64);
        body.extend_from_slice(br#"{"items":["#);
        body.extend_from_slice(&items);
        body.extend_from_slice(br#"],"next":"#);
        body.extend_from_slice(&serde_json::to_vec(&next).expect("a cursor always encodes"));
        body.push(b'}');
        ([(header::CONTENT_TYPE, "application/json")], body).into_response()
    }

    fn cursor(&self, after: &str) -> String {
        let cursor = json!({ "after": after, "filters": self.filters });
        URL_SAFE_NO_PAD.encode(cursor.to_string())
    }
}

fn resume(raw: &str, filters: &Map<String, Value>) -> Result<String, String> {
    let invalid = || "the cursor is not valid".to_owned();
    let decoded = URL_SAFE_NO_PAD.decode(raw).map_err(|_| invalid())?;
    let cursor: Value = serde_json::from_slice(&decoded).map_err(|_| invalid())?;

    let after = cursor
        .get("after")
        .and_then(Value::as_str)
        .ok_or_else(invalid)?;
    let made_for = cursor
        .get("filters")
        .and_then(Value::as_object)
        .ok_or_else(invalid)?;
    if made_for != filters {
        return Err("the cursor belongs to a different query".to_owned());
    }

    Ok(after.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;

    fn uri(query: &str) -> Uri {
        format!("/admin/things?{query}").parse().unwrap()
    }

    async fn body(response: Response) -> Value {
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    fn page(keys: &[&str], next: Option<&str>) -> Page<String, String> {
        Page {
            items: keys
                .iter()
                .map(|key| ((*key).to_owned(), (*key).to_owned()))
                .collect(),
            next: next.map(str::to_owned),
        }
    }

    #[test]
    fn limit_defaults_and_is_bounded() {
        assert_eq!(Paging::from_query(&uri(""), &[]).unwrap().limit, 100);
        assert_eq!(
            Paging::from_query(&uri("limit=1000"), &[]).unwrap().limit,
            1_000
        );
        for bad in ["limit=0", "limit=1001", "limit=-1", "limit=many"] {
            assert!(Paging::from_query(&uri(bad), &[]).is_err(), "{bad}");
        }
    }

    #[tokio::test]
    async fn a_cursor_resumes_the_same_query() {
        let first = Paging::from_query(&uri("prefix=svc"), &["prefix"]).unwrap();
        let reply = body(first.reply(page(&["svc/a"], Some("svc/a")), |_, item| json!(item))).await;
        let cursor = reply["next"].as_str().unwrap();

        let second =
            Paging::from_query(&uri(&format!("prefix=svc&cursor={cursor}")), &["prefix"]).unwrap();
        assert_eq!(second.after.as_deref(), Some("svc/a"));
        assert_eq!(second.filter("prefix"), Some("svc"));
    }

    #[tokio::test]
    async fn a_cursor_is_refused_for_another_query_or_when_tampered() {
        let first = Paging::from_query(&uri("prefix=svc"), &["prefix"]).unwrap();
        let reply = body(first.reply(page(&["svc/a"], Some("svc/a")), |_, item| json!(item))).await;
        let cursor = reply["next"].as_str().unwrap().to_owned();

        let other = Paging::from_query(&uri(&format!("prefix=ops&cursor={cursor}")), &["prefix"]);
        assert_eq!(
            other.unwrap_err(),
            "the cursor belongs to a different query"
        );
        let tampered = Paging::from_query(&uri("cursor=not-a-cursor"), &["prefix"]);
        assert_eq!(tampered.unwrap_err(), "the cursor is not valid");
    }

    #[tokio::test]
    async fn the_last_page_has_no_next() {
        let paging = Paging::from_query(&uri(""), &[]).unwrap();
        let reply = body(paging.reply(page(&["a", "b"], None), |_, item| json!(item))).await;

        assert_eq!(reply["items"], json!(["a", "b"]));
        assert_eq!(reply["next"], Value::Null);
    }

    #[tokio::test]
    async fn a_page_stops_at_the_byte_cap_and_resumes_after_the_last_item_sent() {
        let paging = Paging::from_query(&uri(""), &[]).unwrap();
        let large = "x".repeat(MAX_BYTES / 2);
        let reply = body(paging.reply(page(&["a", "b", "c"], None), |_, _| json!(large))).await;

        assert_eq!(reply["items"].as_array().unwrap().len(), 1);
        let cursor = reply["next"].as_str().unwrap();
        let resumed = Paging::from_query(&uri(&format!("cursor={cursor}")), &[]).unwrap();
        assert_eq!(resumed.after.as_deref(), Some("a"));
    }

    #[tokio::test]
    async fn one_item_larger_than_the_cap_is_still_sent() {
        let paging = Paging::from_query(&uri(""), &[]).unwrap();
        let huge = "x".repeat(MAX_BYTES + 1);
        let reply = body(paging.reply(page(&["a", "b"], None), |_, _| json!(huge))).await;

        assert_eq!(reply["items"].as_array().unwrap().len(), 1);
        assert!(reply["next"].is_string());
    }
}
