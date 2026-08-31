use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use hdrhistogram::sync::{Recorder, SyncHistogram};
use hdrhistogram::Histogram;
use picomq_client::ClientError;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::Message;

use super::{emit, report_progress, BenchArgs, Counters, Schedule};
use crate::io::note;
use crate::stream::Target;

pub(super) async fn run(endpoint: &Target, args: BenchArgs) -> Result<i32, ClientError> {
    let bootstrap = endpoint.endpoint.as_str();
    let streams = args.streams.max(1);
    let connections = args.connections.max(1);
    let in_flight = if args.producer {
        args.in_flight.clamp(1, 5)
    } else {
        args.in_flight.max(1)
    };
    let record_size = args.record_size.max(1);
    let payload = vec![b'x'; record_size];
    let record_bytes = record_size as u64;

    let run_id = format!(
        "{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or_default()
    );
    let topics: Vec<String> = (0..streams)
        .map(|i| format!("bench-{run_id}-{i}"))
        .collect();
    create_topics(bootstrap, &topics).await?;
    note(format!(
        "created {} topic{} for kafka bench",
        topics.len(),
        if topics.len() == 1 { "" } else { "s" },
    ));

    let producers: Vec<FutureProducer> = (0..connections)
        .map(|_| producer(bootstrap, &args, in_flight))
        .collect::<Result<_, _>>()?;

    let write = Arc::new(Counters::default());
    let read = Arc::new(Counters::default());
    let stop = Arc::new(AtomicBool::new(false));
    let sent_bytes = Arc::new(AtomicU64::new(0));
    let failure: Arc<std::sync::Mutex<Option<ClientError>>> = Arc::new(Default::default());
    let mut latency: SyncHistogram<u64> = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3)
        .expect("histogram bounds")
        .into();

    let readers: Vec<_> = if args.no_read {
        Vec::new()
    } else {
        topics
            .iter()
            .map(|topic| {
                let consumer = consumer(bootstrap, &format!("bench-read-{topic}"))?;
                Ok(tokio::spawn(reader(
                    consumer,
                    topic.clone(),
                    read.clone(),
                    stop.clone(),
                )))
            })
            .collect::<Result<Vec<_>, ClientError>>()?
    };

    let started = Instant::now();
    let deadline = started + Duration::from_secs(args.duration.max(1) + args.warmup);
    let target_bytes_per_sec = if args.target_mibps > 0.0 {
        args.target_mibps * 1024.0 * 1024.0
    } else {
        0.0
    };
    let schedule = Schedule {
        started,
        deadline,
        target_bytes_per_sec,
    };

    let outstanding = in_flight * args.batch.max(1);
    let writer_count = connections.max(streams);
    let writers: Vec<_> = (0..writer_count)
        .map(|i| {
            tokio::spawn(writer(
                producers[i % producers.len()].clone(),
                topics[i % topics.len()].clone(),
                payload.clone(),
                record_bytes,
                latency.recorder(),
                write.clone(),
                sent_bytes.clone(),
                failure.clone(),
                schedule,
                outstanding,
            ))
        })
        .collect();

    if args.warmup > 0 {
        tokio::time::sleep(Duration::from_secs(args.warmup)).await;
        write.reset();
        read.reset();
        latency.refresh_timeout(Duration::from_millis(200));
        latency.reset();
        note(format!("warmup of {}s discarded", args.warmup));
    }

    let measured_from = Instant::now();
    report_progress(&mut latency, &write, &read, measured_from, deadline, &args).await;

    for writer in writers {
        let _ = writer.await;
    }
    stop.store(true, Ordering::Relaxed);
    for reader in readers {
        let _ = reader.await;
    }

    let elapsed = measured_from.elapsed();
    latency.refresh_timeout(Duration::from_millis(500));
    let failed = failure.lock().unwrap().take();
    if let Some(error) = &failed {
        note(format!("produce failed: {error}"));
    }
    emit(&args, "kafka", &latency, &write, &read, elapsed);

    if !args.keep {
        delete_topics(bootstrap, &topics).await?;
    }

    Ok(if failed.is_some() { 1 } else { 0 })
}

fn producer(
    bootstrap: &str,
    args: &BenchArgs,
    in_flight: usize,
) -> Result<FutureProducer, ClientError> {
    let batch_size = (args.batch.max(1) * args.record_size.max(1)).clamp(1024, 1_048_576);
    ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("acks", "all")
        .set("linger.ms", "5")
        .set("batch.size", batch_size.to_string())
        .set(
            "max.in.flight.requests.per.connection",
            in_flight.to_string(),
        )
        .set(
            "enable.idempotence",
            if args.producer { "true" } else { "false" },
        )
        .set("message.timeout.ms", "60000")
        .create()
        .map_err(kafka_err)
}

fn consumer(bootstrap: &str, group: &str) -> Result<StreamConsumer, ClientError> {
    ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("group.id", group)
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .create()
        .map_err(kafka_err)
}

async fn create_topics(bootstrap: &str, topics: &[String]) -> Result<(), ClientError> {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .create()
        .map_err(kafka_err)?;
    let new_topics: Vec<NewTopic> = topics
        .iter()
        .map(|name| NewTopic::new(name, 1, TopicReplication::Fixed(1)))
        .collect();
    let results = admin
        .create_topics(new_topics.iter(), &AdminOptions::new())
        .await
        .map_err(kafka_err)?;
    for result in results {
        result.map_err(|(name, err)| kafka_err(format!("{name}: {err}")))?;
    }
    Ok(())
}

async fn delete_topics(bootstrap: &str, topics: &[String]) -> Result<(), ClientError> {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .create()
        .map_err(kafka_err)?;
    let names: Vec<&str> = topics.iter().map(String::as_str).collect();
    let results = admin
        .delete_topics(&names, &AdminOptions::new())
        .await
        .map_err(kafka_err)?;
    for result in results {
        result.map_err(|(name, err)| kafka_err(format!("{name}: {err}")))?;
    }
    Ok(())
}

fn kafka_err(error: impl std::fmt::Display) -> ClientError {
    ClientError::transport(error.to_string())
}

#[allow(clippy::too_many_arguments)]
async fn writer(
    producer: FutureProducer,
    topic: String,
    payload: Vec<u8>,
    record_bytes: u64,
    mut latency: Recorder<u64>,
    write: Arc<Counters>,
    sent_bytes: Arc<AtomicU64>,
    failure: Arc<std::sync::Mutex<Option<ClientError>>>,
    schedule: Schedule,
    outstanding: usize,
) {
    let (done_tx, mut done_rx) = tokio::sync::mpsc::channel(outstanding.max(1));
    let collector = tokio::spawn(async move {
        while let Some((sent, delivery)) = done_rx.recv().await {
            match delivery.await {
                Ok(Ok(_)) => {
                    let micros = Instant::now().duration_since(sent).as_micros().max(1) as u64;
                    let _ = latency.record(micros);
                    write.add(record_bytes, 1);
                }
                Ok(Err((error, _))) => return Some(kafka_err(error)),
                Err(error) => return Some(kafka_err(error)),
            }
        }
        None
    });

    'produce: while Instant::now() < schedule.deadline {
        if failure.lock().unwrap().is_some() {
            break;
        }

        let sent_total = sent_bytes.fetch_add(record_bytes, Ordering::Relaxed) + record_bytes;
        if schedule.target_bytes_per_sec > 0.0 {
            let expected =
                Duration::from_secs_f64(sent_total as f64 / schedule.target_bytes_per_sec);
            let elapsed = schedule.started.elapsed();
            if expected > elapsed {
                tokio::time::sleep(expected - elapsed).await;
            }
        }

        let sent = Instant::now();
        loop {
            match producer
                .send_result(FutureRecord::<(), [u8]>::to(&topic).payload(payload.as_slice()))
            {
                Ok(delivery) => {
                    if done_tx.send((sent, delivery)).await.is_err() {
                        break 'produce;
                    }
                    break;
                }
                Err((error, _))
                    if error.rdkafka_error_code()
                        == Some(rdkafka::types::RDKafkaErrorCode::QueueFull) =>
                {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
                Err((error, _)) => {
                    {
                        let mut slot = failure.lock().unwrap();
                        if slot.is_none() {
                            *slot = Some(kafka_err(error));
                        }
                    }
                    break 'produce;
                }
            }
        }
    }
    drop(done_tx);
    if let Ok(Some(error)) = collector.await {
        let mut slot = failure.lock().unwrap();
        if slot.is_none() {
            *slot = Some(error);
        }
    }
}

async fn reader(
    consumer: StreamConsumer,
    topic: String,
    read: Arc<Counters>,
    stop: Arc<AtomicBool>,
) {
    let mut assignment = rdkafka::TopicPartitionList::new();
    if assignment
        .add_partition_offset(&topic, 0, rdkafka::Offset::Beginning)
        .is_err()
    {
        return;
    }
    if consumer.assign(&assignment).is_err() {
        return;
    }

    while !stop.load(Ordering::Relaxed) {
        match tokio::time::timeout(Duration::from_millis(200), consumer.recv()).await {
            Ok(Ok(message)) => {
                let bytes = message.payload().map(|p| p.len() as u64).unwrap_or(0);
                read.add(bytes, 1);
            }
            Ok(Err(error)) => {
                if !stop.load(Ordering::Relaxed) {
                    note(format!("read failed: {error}"));
                }
                return;
            }
            Err(_) => {}
        }
    }
}
