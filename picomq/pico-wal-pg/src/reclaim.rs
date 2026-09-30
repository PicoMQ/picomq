use std::sync::{Arc, Mutex};

use sqlx::{AssertSqlSafe, PgPool};

use crate::error::Error;
use crate::ring::Ring;
use crate::schema::Schema;

pub(crate) struct Reclaimer {
    pool: PgPool,
    schema: Arc<Schema>,
    ring: Arc<Mutex<Ring>>,
    serial: tokio::sync::Mutex<()>,
}

impl Reclaimer {
    pub(crate) fn new(pool: PgPool, schema: Arc<Schema>, ring: Arc<Mutex<Ring>>) -> Self {
        Self {
            pool,
            schema,
            ring,
            serial: tokio::sync::Mutex::new(()),
        }
    }

    pub(crate) async fn run(&self, watermark: u64, frontier: u64) -> Result<usize, Error> {
        let _serial = self.serial.lock().await;
        let slots = self
            .ring
            .lock()
            .expect("ring poisoned")
            .reclaimable(watermark, frontier);
        for &index in &slots {
            truncate(&self.pool, &self.schema.slots()[index..=index]).await?;
            self.ring.lock().expect("ring poisoned").release(index);
        }
        Ok(slots.len())
    }
}

pub(crate) async fn truncate(pool: &PgPool, tables: &[String]) -> Result<(), Error> {
    if tables.is_empty() {
        return Ok(());
    }
    sqlx::query(AssertSqlSafe(format!("TRUNCATE {}", tables.join(", "))))
        .execute(pool)
        .await?;
    Ok(())
}
