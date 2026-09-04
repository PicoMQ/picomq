use std::io::{self, BufRead, Cursor, Read};
use std::slice::Iter;

use simd_json::OwnedValue;

pub struct JsonArrowReader<'a> {
    values: Iter<'a, &'a OwnedValue>,
    cursor: Cursor<Vec<u8>>,
}

impl<'a> JsonArrowReader<'a> {
    pub fn new(values: &'a [&OwnedValue]) -> Self {
        Self {
            values: values.iter(),
            cursor: Cursor::new(Vec::new()),
        }
    }

    fn load_next(&mut self) -> io::Result<bool> {
        let Some(val) = self.values.next() else {
            return Ok(false);
        };

        let mut buf = Vec::new();
        simd_json::to_writer(&mut buf, val).map_err(io::Error::other)?;
        buf.push(b'\n');
        self.cursor = Cursor::new(buf);
        Ok(true)
    }
}

impl<'a> Read for JsonArrowReader<'a> {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        loop {
            let n = self.cursor.read(out)?;
            if n > 0 {
                return Ok(n);
            }
            if !self.load_next()? {
                return Ok(0);
            }
        }
    }
}

impl<'a> BufRead for JsonArrowReader<'a> {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        loop {
            if self.cursor.position() < self.cursor.get_ref().len() as u64 {
                return Ok(&self.cursor.get_ref()[self.cursor.position() as usize..]);
            }

            if !self.load_next()? {
                return Ok(&[]);
            }
        }
    }

    fn consume(&mut self, amt: usize) {
        self.cursor.consume(amt)
    }
}
