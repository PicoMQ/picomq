use std::io::IoSlice;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use picomq_metrics::Open;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf, ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio_rustls::server::TlsStream;

pub struct Stream {
    io: Io,
    open: Open,
}

enum Io {
    Plain(TcpStream),
    Tls(Box<TlsStream<TcpStream>>),
}

pub struct Reader {
    io: ReadIo,
    _open: Arc<Open>,
}

enum ReadIo {
    Plain(OwnedReadHalf),
    Tls(ReadHalf<Box<TlsStream<TcpStream>>>),
}

pub struct Writer {
    io: WriteIo,
    _open: Arc<Open>,
}

enum WriteIo {
    Plain(OwnedWriteHalf),
    Tls(WriteHalf<Box<TlsStream<TcpStream>>>),
}

impl Stream {
    pub(crate) fn plain(socket: TcpStream, open: Open) -> Self {
        Self {
            io: Io::Plain(socket),
            open,
        }
    }

    pub(crate) fn tls(stream: TlsStream<TcpStream>, open: Open) -> Self {
        Self {
            io: Io::Tls(Box::new(stream)),
            open,
        }
    }

    pub fn is_tls(&self) -> bool {
        matches!(self.io, Io::Tls(_))
    }

    pub fn into_split(self) -> (Reader, Writer) {
        let open = Arc::new(self.open);
        let (read, write) = match self.io {
            Io::Plain(socket) => {
                let (read, write) = socket.into_split();
                (ReadIo::Plain(read), WriteIo::Plain(write))
            }
            Io::Tls(stream) => {
                let (read, write) = tokio::io::split(stream);
                (ReadIo::Tls(read), WriteIo::Tls(write))
            }
        };

        (
            Reader {
                io: read,
                _open: open.clone(),
            },
            Writer {
                io: write,
                _open: open,
            },
        )
    }
}

impl AsyncRead for Stream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match &mut self.get_mut().io {
            Io::Plain(socket) => Pin::new(socket).poll_read(cx, buf),
            Io::Tls(stream) => Pin::new(stream).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for Stream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match &mut self.get_mut().io {
            Io::Plain(socket) => Pin::new(socket).poll_write(cx, buf),
            Io::Tls(stream) => Pin::new(stream).poll_write(cx, buf),
        }
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<std::io::Result<usize>> {
        match &mut self.get_mut().io {
            Io::Plain(socket) => Pin::new(socket).poll_write_vectored(cx, bufs),
            Io::Tls(stream) => Pin::new(stream).poll_write_vectored(cx, bufs),
        }
    }

    fn is_write_vectored(&self) -> bool {
        match &self.io {
            Io::Plain(socket) => socket.is_write_vectored(),
            Io::Tls(stream) => stream.is_write_vectored(),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match &mut self.get_mut().io {
            Io::Plain(socket) => Pin::new(socket).poll_flush(cx),
            Io::Tls(stream) => Pin::new(stream).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match &mut self.get_mut().io {
            Io::Plain(socket) => Pin::new(socket).poll_shutdown(cx),
            Io::Tls(stream) => Pin::new(stream).poll_shutdown(cx),
        }
    }
}

impl AsyncRead for Reader {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match &mut self.get_mut().io {
            ReadIo::Plain(read) => Pin::new(read).poll_read(cx, buf),
            ReadIo::Tls(read) => Pin::new(read).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for Writer {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match &mut self.get_mut().io {
            WriteIo::Plain(write) => Pin::new(write).poll_write(cx, buf),
            WriteIo::Tls(write) => Pin::new(write).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match &mut self.get_mut().io {
            WriteIo::Plain(write) => Pin::new(write).poll_flush(cx),
            WriteIo::Tls(write) => Pin::new(write).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match &mut self.get_mut().io {
            WriteIo::Plain(write) => Pin::new(write).poll_shutdown(cx),
            WriteIo::Tls(write) => Pin::new(write).poll_shutdown(cx),
        }
    }
}
