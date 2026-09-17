use std::collections::BTreeMap;

use super::StreamName;
use super::state::Member;

pub(super) fn assign(members: &BTreeMap<String, Member>) -> BTreeMap<String, Vec<StreamName>> {
    let mut subscribers: BTreeMap<&StreamName, Vec<&str>> = BTreeMap::new();
    for (id, member) in members {
        for stream in member.subscription().into_iter().flatten() {
            subscribers.entry(stream).or_default().push(id);
        }
    }
    let mut assignments: BTreeMap<String, Vec<StreamName>> =
        members.keys().map(|id| (id.clone(), Vec::new())).collect();
    let mut order: Vec<_> = subscribers.into_iter().collect();
    order.sort_by_key(|(stream, candidates)| (candidates.len(), *stream));
    for (stream, candidates) in order {
        let chosen = candidates
            .iter()
            .copied()
            .min_by_key(|id| (assignments[*id].len(), *id))
            .expect("subscribed stream has a subscriber");
        assignments
            .get_mut(chosen)
            .expect("candidate is a member")
            .push(StreamName::clone(stream));
    }
    for streams in assignments.values_mut() {
        streams.sort();
    }
    assignments
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::time::{Duration, Instant};

    use super::super::state::Role;
    use super::*;

    fn member(subscription: &[&str]) -> Member {
        Member {
            instance_id: None,
            client_id: String::new(),
            role: Role::Subscribed {
                subscription: subscription
                    .iter()
                    .map(|s| StreamName::from(*s))
                    .collect::<BTreeSet<_>>(),
                assignment: Vec::new(),
            },
            session_timeout: Duration::from_secs(10),
            rebalance_timeout: Duration::from_secs(10),
            last_heartbeat: Instant::now(),
        }
    }

    fn names(streams: &[StreamName]) -> Vec<&str> {
        streams.iter().map(|s| s.as_ref()).collect()
    }

    #[test]
    fn spreads_streams_evenly() {
        let members = BTreeMap::from([
            ("a".to_owned(), member(&["s1", "s2", "s3", "s4"])),
            ("b".to_owned(), member(&["s1", "s2", "s3", "s4"])),
        ]);
        let assigned = assign(&members);
        assert_eq!(names(&assigned["a"]), ["s1", "s3"]);
        assert_eq!(names(&assigned["b"]), ["s2", "s4"]);
    }

    #[test]
    fn only_subscribers_receive_a_stream() {
        for (narrow, wide) in [("a", "b"), ("b", "a")] {
            let members = BTreeMap::from([
                (narrow.to_owned(), member(&["x"])),
                (wide.to_owned(), member(&["x", "y", "z"])),
            ]);
            let assigned = assign(&members);
            assert_eq!(names(&assigned[narrow]), ["x"]);
            assert_eq!(names(&assigned[wide]), ["y", "z"]);
        }
    }

    #[test]
    fn every_member_appears_even_with_nothing_assigned() {
        let members = BTreeMap::from([
            ("a".to_owned(), member(&["x"])),
            ("b".to_owned(), member(&[])),
        ]);
        let assigned = assign(&members);
        assert_eq!(assigned.len(), 2);
        assert!(assigned["b"].is_empty());
    }
}
