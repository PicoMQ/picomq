# Conformance fixtures

Golden byte vectors from the upstream Java s3stream. Rust tests load them in the normal `cargo test` pass to pin byte compatibility.

Output is deterministic: rerunning produces identical bytes.
