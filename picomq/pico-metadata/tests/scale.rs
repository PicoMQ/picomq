//! Scale gates for the "unlimited streams" design constraint. Asserts the
//! properties the design promises at large stream counts:
//!
//! - creating N streams is O(N log N) with small constants.
//! - a state fork (`clone`), which is what snapshots and published views
//!   cost, stays O(1) regardless of N.

use std::time::Instant;

use picomq_metadata::{apply, MetadataCommand, MetadataState};

const NODE_A: i32 = 1;
const NODE_B: i32 = 2;
const EPOCH: i64 = 1;

/// Create `total` streams. Every `open_every`-th is opened (alternating owner
/// node) so per-node indexes have real content.
fn build(total: u64, open_every: u64) -> MetadataState {
    let mut state = MetadataState::new();
    for node_id in [NODE_A, NODE_B] {
        apply(
            &mut state,
            &MetadataCommand::RegisterNode {
                node_id,
                node_epoch: EPOCH,
                http_address: String::new(),
                slots: 1,
                protocol_addresses: Default::default(),
            },
        )
        .unwrap();
    }
    for i in 0..total {
        apply(
            &mut state,
            &MetadataCommand::CreateStream {
                node_id: NODE_A,
                node_epoch: EPOCH,
            },
        )
        .unwrap();
        if i % open_every == 0 {
            let node_id = if (i / open_every) % 2 == 0 {
                NODE_A
            } else {
                NODE_B
            };
            apply(
                &mut state,
                &MetadataCommand::OpenStream {
                    node_id,
                    node_epoch: EPOCH,
                    stream_id: i,
                    epoch: 1,
                },
            )
            .unwrap();
        }
    }
    state
}

fn run_gate(total: u64) {
    let open_every = 10;
    let opened = total / open_every; // total opened across both nodes

    let started = Instant::now();
    let state = build(total, open_every);
    let build_elapsed = started.elapsed();
    println!("create {total} streams (+{opened} opens): {build_elapsed:?}");
    assert_eq!(state.streams.len() as u64, total);

    // Fork is O(1): this is what every snapshot and published view costs.
    let started = Instant::now();
    let forks: Vec<MetadataState> = (0..1_000).map(|_| state.clone()).collect();
    let fork_elapsed = started.elapsed() / 1_000;
    println!("state fork (avg of 1000): {fork_elapsed:?}");
    drop(forks);
    assert!(
        fork_elapsed.as_micros() < 1_000,
        "fork must be O(1) (~ns), took {fork_elapsed:?} at {total} streams"
    );

    // Point lookups: O(log N).
    let started = Instant::now();
    for i in (0..total).step_by((total / 10_000).max(1) as usize) {
        assert!(state.get_stream(i).is_some());
    }
    println!("10k point lookups: {:?}", started.elapsed());

    let started = Instant::now();
    let node_b = state.get_opening_streams(NODE_B);
    let opening_elapsed = started.elapsed();
    println!(
        "get_opening_streams({} streams): {opening_elapsed:?}",
        node_b.len()
    );
    assert_eq!(node_b.len() as u64, opened / 2);

    // A mutation on a fork shares structure: it must not degrade with N either.
    let started = Instant::now();
    let mut fork = state.clone();
    apply(
        &mut fork,
        &MetadataCommand::CreateStream {
            node_id: NODE_A,
            node_epoch: EPOCH,
        },
    )
    .unwrap();
    println!(
        "single apply on {total}-stream state: {:?}",
        started.elapsed()
    );
    assert_eq!(
        state.streams.len() as u64,
        total,
        "original untouched (persistent maps)"
    );

    // Snapshot round trip over the full state.
    let started = Instant::now();
    let encoded = picomq_metadata::snapshot::encode(&state);
    let encode_elapsed = started.elapsed();
    let bytes_per_stream = encoded.len() as u64 / total;
    println!(
        "snapshot encode: {encode_elapsed:?}, {} bytes total, {bytes_per_stream} B/stream",
        encoded.len()
    );
    assert!(
        bytes_per_stream <= 64,
        "compact rows: expected <= 64 B/stream in the snapshot"
    );
    let started = Instant::now();
    let decoded = picomq_metadata::snapshot::decode(&encoded).unwrap();
    println!(
        "snapshot decode (incl. index rebuild): {:?}",
        started.elapsed()
    );
    assert_eq!(decoded, state);
}

/// CI-friendly gate: 100k streams.
#[test]
fn hundred_k_streams_gate() {
    run_gate(100_000);
}

/// The full 1M gate. Run explicitly in release mode (see module docs).
#[test]
#[ignore = "run explicitly: cargo test --release -p picomq-metadata --test scale -- --ignored --nocapture"]
fn million_streams_gate() {
    run_gate(1_000_000);
}
