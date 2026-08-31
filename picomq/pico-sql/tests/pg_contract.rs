//! Postgres contract test. The same behavioral suite the SQLite backends
//! pass, run against a real Postgres when `PICOMQ_PG_URL` is set, e.g.:
//!
//! ```text
//! PICOMQ_PG_URL=postgres://user:pass@localhost:5432/picomq \
//!     cargo test -p picomq-sql --test pg_contract
//! ```
//!
//! Skipped (pass, with a note) when the variable is absent, so CI without a
//! database stays green. The test WIPES the three metadata tables first.
//! Point it at a dedicated test database.

use picomq_sql::store::contract_suite;
use picomq_sql::PgStore;

#[tokio::test]
async fn postgres_contract() {
    let Ok(url) = std::env::var("PICOMQ_PG_URL") else {
        eprintln!("PICOMQ_PG_URL not set; skipping postgres contract test");
        return;
    };

    // Fresh slate: the contract suite assumes an empty store.
    let admin = sqlx::PgPool::connect(&url)
        .await
        .expect("connect for cleanup");
    sqlx::query("DROP TABLE IF EXISTS meta_log, meta_snapshot, meta_lease")
        .execute(&admin)
        .await
        .expect("drop tables");
    admin.close().await;

    let store = PgStore::connect(&url).await.expect("connect + migrate");
    contract_suite(&store).await;
}
