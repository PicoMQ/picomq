mod common;

use std::time::Duration;

use s3stream_wal::{WalError, WriteAheadLog};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;

use common::{cluster, record, url, wal};

struct Proxy {
    url: String,
    up: watch::Sender<bool>,
}

impl Proxy {
    async fn start(url: &str) -> Self {
        let mut through = url::Url::parse(url).unwrap();
        let upstream = format!(
            "{}:{}",
            through.host_str().unwrap(),
            through.port().unwrap_or(5432)
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        through.set_host(Some("127.0.0.1")).unwrap();
        through
            .set_port(Some(listener.local_addr().unwrap().port()))
            .unwrap();
        let (up, _) = watch::channel(true);
        let links = up.clone();
        tokio::spawn(async move {
            loop {
                let (mut client, _) = listener.accept().await.unwrap();
                let mut alive = links.subscribe();
                if !*alive.borrow_and_update() {
                    continue;
                }
                let upstream = upstream.clone();
                tokio::spawn(async move {
                    let Ok(mut server) = TcpStream::connect(&upstream).await else {
                        return;
                    };
                    tokio::select! {
                        _ = tokio::io::copy_bidirectional(&mut client, &mut server) => {}
                        _ = alive.wait_for(|up| !*up) => {}
                    }
                });
            }
        });
        Self {
            url: through.into(),
            up,
        }
    }

    fn cut(&self) {
        self.up.send_replace(false);
    }

    fn restore(&self) {
        self.up.send_replace(true);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "run explicitly: PICO_TEST_PG_URL=postgres://... cargo test -p picomq-pgwal --test outage -- --ignored"]
async fn waits_out_a_postgres_outage() {
    let proxy = Proxy::start(&url()).await;
    let wal = wal(&proxy.url, &cluster(), 1, "batchInterval=0");
    wal.start().await.unwrap();
    wal.submit(record(9, 0, b"before"))
        .unwrap()
        .durable
        .await
        .unwrap();

    proxy.cut();
    let stalled = wal.submit(record(9, 1, b"during")).unwrap();
    tokio::time::sleep(Duration::from_secs(35)).await;
    proxy.restore();
    tokio::time::timeout(Duration::from_secs(15), stalled.durable)
        .await
        .expect("the append never recovered from the outage")
        .unwrap();

    proxy.cut();
    let abandoned = wal.submit(record(9, 2, b"abandoned")).unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;
    tokio::time::timeout(Duration::from_secs(10), wal.shutdown_gracefully())
        .await
        .expect("shutdown hung on a dead database");
    assert!(matches!(abandoned.durable.await, Err(WalError::Shutdown)));
}
