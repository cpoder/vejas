use std::convert::TryFrom;
use std::io::Result as IoResult;
use std::io::{Error as IoError, ErrorKind, Read, Write};
use std::net::{Shutdown, SocketAddr};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::connection::Connection;
#[cfg(any(feature = "ssl-openssl", feature = "ssl-rustls"))]
use crate::ssl::SslStream;

pub(crate) enum Stream {
    Http(Connection),
    #[cfg(any(feature = "ssl-openssl", feature = "ssl-rustls"))]
    Https(SslStream),
}

impl Clone for Stream {
    fn clone(&self) -> Self {
        match self {
            Stream::Http(tcp_stream) => Stream::Http(tcp_stream.try_clone().unwrap()),
            #[cfg(any(feature = "ssl-openssl", feature = "ssl-rustls"))]
            Stream::Https(ssl_stream) => Stream::Https(ssl_stream.clone()),
        }
    }
}

impl From<Connection> for Stream {
    fn from(tcp_stream: Connection) -> Self {
        Stream::Http(tcp_stream)
    }
}

impl Stream {
    /// Vejas patch: the socket's timeouts (a no-op over TLS).
    fn set_read_timeout(&self, t: Duration) {
        match self {
            Stream::Http(c) => c.set_read_timeout(t),
            #[cfg(any(feature = "ssl-openssl", feature = "ssl-rustls"))]
            Stream::Https(_) => (),
        }
    }

    fn set_write_timeout(&self, t: Duration) {
        match self {
            Stream::Http(c) => c.set_write_timeout(t),
            #[cfg(any(feature = "ssl-openssl", feature = "ssl-rustls"))]
            Stream::Https(_) => (),
        }
    }

    fn secure(&self) -> bool {
        match self {
            Stream::Http(_) => false,
            #[cfg(any(feature = "ssl-openssl", feature = "ssl-rustls"))]
            Stream::Https(_) => true,
        }
    }

    fn peer_addr(&mut self) -> IoResult<Option<SocketAddr>> {
        match self {
            Stream::Http(tcp_stream) => tcp_stream.peer_addr(),
            #[cfg(any(feature = "ssl-openssl", feature = "ssl-rustls"))]
            Stream::Https(ssl_stream) => ssl_stream.peer_addr(),
        }
    }

    fn shutdown(&mut self, how: Shutdown) -> IoResult<()> {
        match self {
            Stream::Http(tcp_stream) => tcp_stream.shutdown(how),
            #[cfg(any(feature = "ssl-openssl", feature = "ssl-rustls"))]
            Stream::Https(ssl_stream) => ssl_stream.shutdown(how),
        }
    }
}

impl Read for Stream {
    fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
        match self {
            Stream::Http(tcp_stream) => tcp_stream.read(buf),
            #[cfg(any(feature = "ssl-openssl", feature = "ssl-rustls"))]
            Stream::Https(ssl_stream) => ssl_stream.read(buf),
        }
    }
}

impl Write for Stream {
    fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
        match self {
            Stream::Http(tcp_stream) => tcp_stream.write(buf),
            #[cfg(any(feature = "ssl-openssl", feature = "ssl-rustls"))]
            Stream::Https(ssl_stream) => ssl_stream.write(buf),
        }
    }

    fn flush(&mut self) -> IoResult<()> {
        match self {
            Stream::Http(tcp_stream) => tcp_stream.flush(),
            #[cfg(any(feature = "ssl-openssl", feature = "ssl-rustls"))]
            Stream::Https(ssl_stream) => ssl_stream.flush(),
        }
    }
}

pub struct RefinedTcpStream {
    stream: Stream,
    close_read: bool,
    close_write: bool,
    clock: Option<Arc<Clock>>,
}

/// Vejas patch: the slowest a response may be written, once its first byte is,
/// on top of the timeout: a client must read at least this many bytes a second.
const MIN_WRITE_RATE: u64 = 16 * 1024;

/// Vejas patch: the deadlines a connection's reads and writes are held to.
///
/// A read waits at most until the read deadline the connection set (for the
/// request head and body together), never past it: the socket's timeout is
/// set to what is left before each read. A write (a response, up to its
/// flush) may take the timeout plus one second per `MIN_WRITE_RATE` bytes it
/// has written, in all, however it is spread. Each read and write also waits
/// at most the timeout. Over TLS, only the socket's own timeouts apply.
pub(crate) struct Clock {
    base: Instant,
    timeout: Option<Duration>,
    /// nanoseconds after `base`, plus one; 0: none
    read_by: AtomicU64,
    /// when the write in progress began (as above); 0: none
    write_from: AtomicU64,
    written: AtomicU64,
}

impl Clock {
    pub(crate) fn new(timeout: Option<Duration>) -> Arc<Clock> {
        Arc::new(Clock {
            base: Instant::now(),
            timeout,
            read_by: AtomicU64::new(0),
            write_from: AtomicU64::new(0),
            written: AtomicU64::new(0),
        })
    }

    fn stamp(&self, at: Instant) -> u64 {
        let n = at.saturating_duration_since(self.base).as_nanos();
        u64::try_from(n).unwrap_or(u64::MAX - 1).saturating_add(1)
    }

    fn at(&self, stamp: u64) -> Option<Instant> {
        match stamp {
            0 => None,
            n => self.base.checked_add(Duration::from_nanos(n - 1)),
        }
    }

    /// Reads from now on must be done within `d` (the timeout when `None`).
    /// Returns the deadline set.
    pub(crate) fn read_within(&self, d: Option<Duration>) -> Option<Instant> {
        let by = d
            .or(self.timeout)
            .and_then(|d| Instant::now().checked_add(d));
        self.read_by.store(by.map_or(0, |b| self.stamp(b)), Ordering::SeqCst);
        by
    }

    /// What a read may still wait; past the deadline, `TimedOut`.
    fn read_budget(&self) -> IoResult<Option<Duration>> {
        let left = match self.at(self.read_by.load(Ordering::SeqCst)) {
            Some(by) => match by.checked_duration_since(Instant::now()) {
                Some(left) if !left.is_zero() => Some(left),
                _ => return Err(IoError::new(ErrorKind::TimedOut, "read deadline passed")),
            },
            None => None,
        };
        Ok(min_some(left, self.timeout))
    }

    /// What a write may still wait; past the allowance, `TimedOut`.
    fn write_budget(&self) -> IoResult<Option<Duration>> {
        let timeout = match self.timeout {
            Some(t) => t,
            None => return Ok(None),
        };
        let now = Instant::now();
        let from = match self.at(self.write_from.load(Ordering::SeqCst)) {
            Some(from) => from,
            None => {
                self.write_from.store(self.stamp(now), Ordering::SeqCst);
                self.written.store(0, Ordering::SeqCst);
                now
            }
        };
        let rate = Duration::from_secs(self.written.load(Ordering::SeqCst) / MIN_WRITE_RATE);
        let by = from.checked_add(timeout.saturating_add(rate));
        match by.and_then(|by| by.checked_duration_since(now)) {
            Some(left) if !left.is_zero() => Ok(Some(left.min(timeout))),
            None if by.is_none() => Ok(Some(timeout)),
            _ => Err(IoError::new(ErrorKind::TimedOut, "write deadline passed")),
        }
    }
}

fn min_some(a: Option<Duration>, b: Option<Duration>) -> Option<Duration> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, None) => a,
        (None, b) => b,
    }
}

/// Vejas patch: a socket timeout on Linux reads as `WouldBlock`; it is a timeout.
fn timed_out(e: IoError) -> IoError {
    if e.kind() == ErrorKind::WouldBlock {
        IoError::new(ErrorKind::TimedOut, e)
    } else {
        e
    }
}

impl RefinedTcpStream {
    pub(crate) fn new<S>(
        stream: S,
        clock: Option<Arc<Clock>>,
    ) -> (RefinedTcpStream, RefinedTcpStream)
    where
        S: Into<Stream>,
    {
        let stream: Stream = stream.into();

        let (read, write) = (stream.clone(), stream);

        let read = RefinedTcpStream {
            stream: read,
            close_read: true,
            close_write: false,
            clock: clock.clone(),
        };

        let write = RefinedTcpStream {
            stream: write,
            close_read: false,
            close_write: true,
            clock,
        };

        (read, write)
    }

    /// Returns true if this struct wraps around a secure connection.
    #[inline]
    pub(crate) fn secure(&self) -> bool {
        self.stream.secure()
    }

    pub(crate) fn peer_addr(&mut self) -> IoResult<Option<SocketAddr>> {
        self.stream.peer_addr()
    }

    /// Vejas patch: the clock this connection's reads and writes are held to.
    pub(crate) fn clock(&self) -> Option<Arc<Clock>> {
        self.clock.clone()
    }
}

impl Drop for RefinedTcpStream {
    fn drop(&mut self) {
        if self.close_read {
            self.stream.shutdown(Shutdown::Read).ok();
        }

        if self.close_write {
            self.stream.shutdown(Shutdown::Write).ok();
        }
    }
}

impl Read for RefinedTcpStream {
    fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
        if let Some(clock) = &self.clock {
            if let Some(wait) = clock.read_budget()? {
                self.stream.set_read_timeout(wait);
            }
        }
        self.stream.read(buf).map_err(timed_out)
    }
}

impl Write for RefinedTcpStream {
    fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
        if let Some(clock) = &self.clock {
            if let Some(wait) = clock.write_budget()? {
                self.stream.set_write_timeout(wait);
            }
        }
        let n = self.stream.write(buf).map_err(timed_out)?;
        if let Some(clock) = &self.clock {
            clock.written.fetch_add(n as u64, Ordering::SeqCst);
        }
        Ok(n)
    }

    fn flush(&mut self) -> IoResult<()> {
        let done = self.stream.flush().map_err(timed_out);
        // the write in progress is over: the next one has its own allowance
        if let Some(clock) = &self.clock {
            clock.write_from.store(0, Ordering::SeqCst);
        }
        done
    }
}
