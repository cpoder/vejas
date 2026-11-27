//! Vejas patch (see README.md at the crate root): the bounds on a request body.
//!
//! Upstream 0.12.0 drained an unread body on drop with one allocation of the
//! whole declared length, read chunked bodies with no bound, and never gave up
//! on a client that sent slowly. A `BodyGuard` reads at most `max` bytes, gives
//! up at `deadline`, and drains what the handler left unread into a fixed
//! buffer under the same bounds. A body it does not read to its end marks the
//! connection abandoned: what follows on the wire is not a next request, and
//! the connection closes instead of reading one there.

use std::error::Error;
use std::fmt;
use std::io::{self, ErrorKind, Read};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

/// Set when a request on a connection left its body unread: the connection
/// reads no further request and closes.
#[derive(Clone, Default)]
pub(crate) struct Abandon(Option<Arc<AtomicBool>>);

impl Abandon {
    pub(crate) fn new() -> Self {
        Abandon(Some(Arc::new(AtomicBool::new(false))))
    }

    pub(crate) fn set(&self) {
        if let Some(flag) = &self.0 {
            flag.store(true, Ordering::SeqCst);
        }
    }

    pub(crate) fn is_set(&self) -> bool {
        self.0.as_ref().map_or(false, |f| f.load(Ordering::SeqCst))
    }
}

/// The error a body read returns once the body passes the server's bound.
/// It travels inside an `io::Error` of kind `InvalidData`; a handler finds it
/// with `e.get_ref().map_or(false, |e| e.is::<BodyTooLarge>())`.
#[derive(Debug)]
pub struct BodyTooLarge {
    /// The bound the body passed, in bytes.
    pub max: usize,
}

impl fmt::Display for BodyTooLarge {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "request body larger than {} bytes", self.max)
    }
}

impl Error for BodyTooLarge {}

pub(crate) struct BodyGuard<R: Read> {
    inner: R,
    read: usize,
    max: usize,
    deadline: Option<Instant>,
    /// false: never read on drop, abandon the connection instead.
    drain: bool,
    /// The inner reader said EOF: the body is read to its end.
    done: bool,
    abandon: Abandon,
}

/// Size of the drain buffer: fixed, whatever the declared length.
const DRAIN_CHUNK: usize = 8 * 1024;

impl<R: Read> BodyGuard<R> {
    pub(crate) fn new(
        inner: R,
        max: usize,
        deadline: Option<Instant>,
        drain: bool,
        abandon: Abandon,
    ) -> Self {
        BodyGuard {
            inner,
            read: 0,
            max,
            deadline,
            drain,
            done: false,
            abandon,
        }
    }

    fn late(&self) -> bool {
        self.deadline.map_or(false, |d| Instant::now() >= d)
    }
}

impl<R: Read> Read for BodyGuard<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.done || buf.is_empty() {
            return Ok(0);
        }
        if self.read > self.max {
            return Err(io::Error::new(ErrorKind::InvalidData, BodyTooLarge { max: self.max }));
        }
        if self.late() {
            return Err(io::Error::new(ErrorKind::TimedOut, "request body too slow"));
        }
        // One byte past the bound is enough to know the body passes it.
        let room = (self.max - self.read).saturating_add(1);
        let len = buf.len().min(room);
        let got = self.inner.read(&mut buf[..len])?;
        if got == 0 {
            self.done = true;
            return Ok(0);
        }
        self.read += got;
        if self.read > self.max {
            return Err(io::Error::new(ErrorKind::InvalidData, BodyTooLarge { max: self.max }));
        }
        Ok(got)
    }
}

impl<R: Read> Drop for BodyGuard<R> {
    fn drop(&mut self) {
        if self.done {
            return;
        }
        // Set already (a caller chose to close rather than drain): no read.
        if self.drain && !self.abandon.is_set() {
            let mut chunk = [0u8; DRAIN_CHUNK];
            while self.read <= self.max && !self.late() {
                match self.inner.read(&mut chunk) {
                    Ok(0) => {
                        self.done = true;
                        break;
                    }
                    Ok(n) => self.read += n,
                    Err(e) if e.kind() == ErrorKind::Interrupted => {}
                    Err(_) => break,
                }
            }
        }
        if !self.done {
            self.abandon.set();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Abandon, BodyGuard, BodyTooLarge};
    use std::io::{Cursor, ErrorKind, Read};
    use std::time::{Duration, Instant};

    #[test]
    fn reads_up_to_the_bound() {
        let a = Abandon::new();
        let mut g = BodyGuard::new(Cursor::new(vec![7u8; 10]), 10, None, true, a.clone());
        let mut out = Vec::new();
        g.read_to_end(&mut out).unwrap();
        assert_eq!(out.len(), 10);
        drop(g);
        assert!(!a.is_set());
    }

    #[test]
    fn past_the_bound_is_an_error_and_abandons() {
        let a = Abandon::new();
        let mut g = BodyGuard::new(Cursor::new(vec![7u8; 11]), 10, None, true, a.clone());
        let mut out = Vec::new();
        let e = g.read_to_end(&mut out).unwrap_err();
        assert_eq!(e.kind(), ErrorKind::InvalidData);
        assert!(e.get_ref().unwrap().is::<BodyTooLarge>());
        drop(g);
        assert!(a.is_set());
    }

    #[test]
    fn drop_drains_what_was_left_within_the_bound() {
        let a = Abandon::new();
        let mut src = Cursor::new(vec![1u8; 100_000]);
        drop(BodyGuard::new(&mut src, 200_000, None, true, a.clone()));
        assert_eq!(src.position(), 100_000);
        assert!(!a.is_set());
    }

    #[test]
    fn drop_stops_at_the_bound_and_abandons() {
        let a = Abandon::new();
        let mut src = Cursor::new(vec![1u8; 100_000]);
        drop(BodyGuard::new(&mut src, 10_000, None, true, a.clone()));
        assert!(src.position() <= 10_000 + 8 * 1024);
        assert!(a.is_set());
    }

    #[test]
    fn no_drain_abandons_without_reading() {
        let a = Abandon::new();
        let mut src = Cursor::new(vec![1u8; 100]);
        drop(BodyGuard::new(&mut src, 1000, None, false, a.clone()));
        assert_eq!(src.position(), 0);
        assert!(a.is_set());
    }

    #[test]
    fn past_the_deadline_is_a_timeout() {
        let a = Abandon::new();
        let past = Instant::now() - Duration::from_millis(1);
        let mut g = BodyGuard::new(Cursor::new(vec![1u8; 100]), 1000, Some(past), true, a.clone());
        let mut b = [0u8; 10];
        assert_eq!(g.read(&mut b).unwrap_err().kind(), ErrorKind::TimedOut);
        drop(g);
        assert!(a.is_set());
    }
}
