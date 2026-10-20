//! Owners learned from ownership redirects, so later requests for the same
//! stream partition go straight to the node that serves it.
//!
//! An owner is kept as the base URL its redirect pointed at, which may carry
//! a path prefix when nodes sit behind a gateway. Entries are keyed by the
//! stream path relative to this client's endpoint.

use std::collections::HashMap;
use std::sync::Mutex;

use reqwest::Url;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Key {
    path: String,
    partition: Option<String>,
}

pub(crate) struct Routes {
    prefix: String,
    owners: Mutex<HashMap<String, HashMap<Option<String>, Url>>>,
}

impl Routes {
    pub(crate) fn new(endpoint: &str) -> Self {
        let prefix = Url::parse(endpoint)
            .map(|url| url.path().trim_end_matches('/').to_owned())
            .unwrap_or_default();

        Self {
            prefix,
            owners: Mutex::default(),
        }
    }

    /// The key of a request this client built, or `None` for a URL outside
    /// its endpoint.
    pub(crate) fn key(&self, url: &Url) -> Option<Key> {
        let path = url.path().strip_prefix(&self.prefix)?;

        Some(Key {
            path: path.to_owned(),
            partition: partition(url),
        })
    }

    pub(crate) fn resolve(&self, key: &Key, url: &mut Url) {
        let owners = self.owners.lock().unwrap();
        let Some(owner) = owners.get(&key.path).and_then(|by| by.get(&key.partition)) else {
            return;
        };

        let mut routed = owner.clone();
        routed.set_path(&format!(
            "{}{}",
            owner.path().trim_end_matches('/'),
            key.path
        ));
        routed.set_query(url.query());
        *url = routed;
    }

    /// Learns the owner a redirect pointed at. A Location that does not end
    /// in the stream path has no recognizable base, so it is not cached.
    pub(crate) fn learn(&self, key: &Key, location: &Url) {
        let Some(base) = location.path().strip_suffix(&key.path) else {
            return;
        };

        let mut owner = location.clone();
        owner.set_path(base);
        owner.set_query(None);

        self.owners
            .lock()
            .unwrap()
            .entry(key.path.clone())
            .or_default()
            .insert(key.partition.clone(), owner);
    }

    /// Drops every partition of the key's stream.
    pub(crate) fn forget(&self, key: &Key) {
        self.owners.lock().unwrap().remove(&key.path);
    }
}

fn partition(url: &Url) -> Option<String> {
    url.query_pairs()
        .find(|(name, _)| name == "partition")
        .map(|(_, value)| value.into_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn url(text: &str) -> Url {
        Url::parse(text).unwrap()
    }

    fn resolved(routes: &Routes, text: &str) -> String {
        let mut request = url(text);
        let key = routes.key(&request).unwrap();
        routes.resolve(&key, &mut request);
        request.to_string()
    }

    #[test]
    fn a_learned_owner_serves_later_requests_for_the_same_partition() {
        let routes = Routes::new("http://node1:4437");
        let requested = url("http://node1:4437/orders?partition=1&seq=0");
        routes.learn(
            &routes.key(&requested).unwrap(),
            &url("http://node2:4437/orders?partition=1&seq=0"),
        );

        assert_eq!(
            resolved(&routes, "http://node1:4437/orders?seq=9&partition=1"),
            "http://node2:4437/orders?seq=9&partition=1"
        );
        assert_eq!(
            resolved(&routes, "http://node1:4437/orders?partition=0"),
            "http://node1:4437/orders?partition=0"
        );
    }

    #[test]
    fn owners_behind_a_path_prefix_keep_it() {
        let routes = Routes::new("https://gw/node1/");
        let requested = url("https://gw/node1/orders?seq=0");
        routes.learn(
            &routes.key(&requested).unwrap(),
            &url("https://gw/node2/orders?seq=0"),
        );

        assert_eq!(
            resolved(&routes, "https://gw/node1/orders?seq=7"),
            "https://gw/node2/orders?seq=7"
        );
    }

    #[test]
    fn a_location_without_the_stream_path_is_not_cached() {
        let routes = Routes::new("http://node1");
        let requested = url("http://node1/orders");
        routes.learn(
            &routes.key(&requested).unwrap(),
            &url("http://node2/elsewhere"),
        );

        assert_eq!(
            resolved(&routes, "http://node1/orders"),
            "http://node1/orders"
        );
    }

    #[test]
    fn forgetting_a_path_drops_every_partition() {
        let routes = Routes::new("http://node1");
        for partition in ["0", "1"] {
            let requested = url(&format!("http://node1/orders?partition={partition}"));
            routes.learn(
                &routes.key(&requested).unwrap(),
                &url(&format!("http://node2/orders?partition={partition}")),
            );
        }
        routes.forget(&routes.key(&url("http://node1/orders")).unwrap());

        assert_eq!(
            resolved(&routes, "http://node1/orders?partition=1"),
            "http://node1/orders?partition=1"
        );
    }
}
