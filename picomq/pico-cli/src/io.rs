//! Terminal input and output.
//!

use std::io::{BufRead, Write};

use bytes::Bytes;
use picomq_client::Record;

/// Read stdin as newline-delimited records, collected rather than streamed
/// so a batch can be appended in one request.
pub fn stdin_records(batch: usize) -> std::io::Result<Vec<Vec<Bytes>>> {
    let stdin = std::io::stdin();
    let mut batches = Vec::new();
    let mut current = Vec::with_capacity(batch);

    for line in stdin.lock().lines() {
        current.push(Bytes::from(line?.into_bytes()));
        if current.len() == batch {
            batches.push(std::mem::take(&mut current));
            current = Vec::with_capacity(batch);
        }
    }
    if !current.is_empty() {
        batches.push(current);
    }
    Ok(batches)
}

/// Bodies that are not UTF-8 print
/// as a byte count instead of corrupting the terminal.
pub fn print_record(record: &Record) {
    let mut out = std::io::stdout().lock();
    match std::str::from_utf8(&record.body) {
        Ok(text) => {
            let _ = writeln!(out, "{}\t{}", record.position, text);
        }
        Err(_) => {
            let _ = writeln!(out, "{}\t<{} bytes>", record.position, record.body.len());
        }
    }
}

pub fn note(line: impl AsRef<str>) {
    eprintln!("{}", line.as_ref());
}
