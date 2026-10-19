//! Owners learned from ownership redirects, so later requests for the same
//! stream partition go straight to the node that serves it.

use std::collections::HashMap;
use std::sync::Mutex;

use reqwest::Url;

#[derive(Default)]
pub(crate) struct Routes {
    owners: Mutex<HashMap<String, HashMap<Option<String>, Url>>>,
}

impl Routes {
    pub(crate) fn resolve(&self, url: &mut Url) {
        let owners = self.owners.lock().unwrap();
        let Some(owner) = owners
            .get(url.path())
            .and_then(|by| by.get(&partition(url)))
        else {
            return;
        };

        let mut routed = owner.clone();
        routed.set_path(url.path());
        routed.set_query(url.query());
        *url = routed;
    }

    pub(crate) fn learn(&self, requested: &Url, location: &Url) {
        let mut owner = location.clone();
        owner.set_path("");
        owner.set_query(None);

        self.owners
            .lock()
            .unwrap()
            .entry(requested.path().to_owned())
            .or_default()
            .insert(partition(requested), owner);
    }

    pub(crate) fn forget(&self, path: &str) {
        self.owners.lock().unwrap().remove(path);
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

    #[test]
    fn a_learned_owner_serves_later_requests_for_the_same_partition() {
        let routes = Routes::default();
        routes.learn(
            &url("http://node1:4437/orders?partition=1&seq=0"),
            &url("http://node2:4437/orders?partition=1&seq=0"),
        );

        let mut read = url("http://node1:4437/orders?seq=9&partition=1");
        routes.resolve(&mut read);
        assert_eq!(read.as_str(), "http://node2:4437/orders?seq=9&partition=1");

        let mut other = url("http://node1:4437/orders?partition=0");
        routes.resolve(&mut other);
        assert_eq!(other.as_str(), "http://node1:4437/orders?partition=0");
    }

    #[test]
    fn forgetting_a_path_drops_every_partition() {
        let routes = Routes::default();
        for partition in ["0", "1"] {
            routes.learn(
                &url(&format!("http://node1/orders?partition={partition}")),
                &url(&format!("http://node2/orders?partition={partition}")),
            );
        }
        routes.forget("/orders");

        let mut request = url("http://node1/orders?partition=1");
        routes.resolve(&mut request);
        assert_eq!(request.host_str(), Some("node1"));
    }
}
