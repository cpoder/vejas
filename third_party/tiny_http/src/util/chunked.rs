//! Vejas patch (see README.md at the crate root): a chunked body reader with
//! bounds, in place of `chunked_transfer::Decoder`.
//!
//! The decoder of chunked_transfer 1.5 read a chunk-size line into a `Vec`
//! until it met `\r`, with no bound and no clock: an endless line of `1`s grew
//! memory for as long as the client sent it. This reader holds every line (the
//! size, its extensions, each trailer) to `MAX_LINE`, the trailers to
//! `MAX_TRAILERS`, counts every byte on the wire (sizes, extensions, CRLFs,
//! data, trailers) against `limit`, and checks `deadline` before every read
//! from the socket.

use std::convert::TryFrom;
use std::io::{self, ErrorKind, Read};
use std::time::Instant;

use crate::util::BodyTooLarge;

/// The longest chunk-size line, extensions included, and the longest trailer.
const MAX_LINE: usize = 4 * 1024;
/// All the trailers together.
const MAX_TRAILERS: usize = 8 * 1024;

#[derive(Debug, Clone, Copy, PartialEq)]
enum State {
    /// Before a chunk-size line.
    Size,
    /// Inside a chunk, this many bytes left.
    Data(u64),
    /// After a chunk's data: its CRLF.
    DataEnd,
    /// After the last chunk: trailers, then an empty line.
    Trailers,
    Done,
}

pub(crate) struct ChunkedReader<R: Read> {
    inner: R,
    state: State,
    wire: usize,
    limit: usize,
    max_body: usize,
    trailers: usize,
    deadline: Option<Instant>,
}

fn bad(what: &str) -> io::Error {
    io::Error::new(ErrorKind::InvalidData, format!("bad chunked body: {}", what))
}

impl<R: Read> ChunkedReader<R> {
    /// `limit`: wire bytes at most; past it the read fails with
    /// [`BodyTooLarge`] naming `max_body`.
    pub(crate) fn new(inner: R, limit: usize, max_body: usize, deadline: Option<Instant>) -> Self {
        ChunkedReader {
            inner,
            state: State::Size,
            wire: 0,
            limit,
            max_body,
            trailers: 0,
            deadline,
        }
    }

    fn count(&mut self, n: usize) -> io::Result<()> {
        self.wire = self.wire.saturating_add(n);
        if self.wire > self.limit {
            return Err(io::Error::new(
                ErrorKind::InvalidData,
                BodyTooLarge { max: self.max_body },
            ));
        }
        Ok(())
    }

    fn check_clock(&self) -> io::Result<()> {
        if self.deadline.map_or(false, |d| Instant::now() >= d) {
            return Err(io::Error::new(ErrorKind::TimedOut, "request body too slow"));
        }
        Ok(())
    }

    fn byte(&mut self) -> io::Result<u8> {
        let mut b = [0u8; 1];
        loop {
            self.check_clock()?;
            match self.inner.read(&mut b) {
                Ok(0) => return Err(io::Error::new(ErrorKind::UnexpectedEof, "chunked body cut short")),
                Ok(_) => {
                    self.count(1)?;
                    return Ok(b[0]);
                }
                Err(e) if e.kind() == ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
    }

    /// One line, without its CRLF, at most `MAX_LINE` bytes. A bare LF is an
    /// error: only CRLF ends a line.
    fn line(&mut self) -> io::Result<Vec<u8>> {
        let mut line = Vec::new();
        loop {
            let b = self.byte()?;
            match b {
                b'\r' => {
                    return if self.byte()? == b'\n' {
                        Ok(line)
                    } else {
                        Err(bad("CR without LF"))
                    };
                }
                b'\n' => return Err(bad("bare LF")),
                _ => {
                    if line.len() >= MAX_LINE {
                        return Err(bad("line too long"));
                    }
                    line.push(b);
                }
            }
        }
    }

    fn size_line(&mut self) -> io::Result<u64> {
        let line = self.line()?;
        // the size, then optional `;extension`s, which are ignored
        let size = match line.iter().position(|&b| b == b';') {
            Some(at) => &line[..at],
            None => &line[..],
        };
        let size = trim(size);
        if size.is_empty() || size.len() > 16 || !size.iter().all(|b| b.is_ascii_hexdigit()) {
            return Err(bad("chunk size is not a hex number"));
        }
        let text = std::str::from_utf8(size).map_err(|_| bad("chunk size"))?;
        u64::from_str_radix(text, 16).map_err(|_| bad("chunk size"))
    }
}

fn trim(mut s: &[u8]) -> &[u8] {
    while let [b' ' | b'\t', rest @ ..] = s {
        s = rest;
    }
    while let [rest @ .., b' ' | b'\t'] = s {
        s = rest;
    }
    s
}

impl<R: Read> Read for ChunkedReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        loop {
            match self.state {
                State::Done => return Ok(0),
                State::Size => {
                    let size = self.size_line()?;
                    self.state = if size == 0 { State::Trailers } else { State::Data(size) };
                }
                State::Data(left) => {
                    self.check_clock()?;
                    let want = buf.len().min(usize::try_from(left).unwrap_or(usize::MAX));
                    // never more than the limit lets through
                    let room = self.limit.saturating_sub(self.wire).saturating_add(1);
                    let want = want.min(room);
                    let got = loop {
                        match self.inner.read(&mut buf[..want]) {
                            Err(e) if e.kind() == ErrorKind::Interrupted => {}
                            other => break other?,
                        }
                    };
                    if got == 0 {
                        return Err(io::Error::new(ErrorKind::UnexpectedEof, "chunked body cut short"));
                    }
                    self.count(got)?;
                    let left = left - got as u64;
                    self.state = if left == 0 { State::DataEnd } else { State::Data(left) };
                    return Ok(got);
                }
                State::DataEnd => {
                    if !self.line()?.is_empty() {
                        return Err(bad("chunk longer than its size"));
                    }
                    self.state = State::Size;
                }
                State::Trailers => {
                    let line = self.line()?;
                    if line.is_empty() {
                        self.state = State::Done;
                        return Ok(0);
                    }
                    self.trailers += line.len() + 2;
                    if self.trailers > MAX_TRAILERS {
                        return Err(bad("trailers too long"));
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ChunkedReader;
    use crate::util::BodyTooLarge;
    use std::io::{Cursor, ErrorKind, Read};
    use std::time::{Duration, Instant};

    fn decode(wire: &[u8], limit: usize) -> std::io::Result<Vec<u8>> {
        let mut r = ChunkedReader::new(Cursor::new(wire.to_vec()), limit, limit, None);
        let mut out = Vec::new();
        r.read_to_end(&mut out).map(|_| out)
    }

    #[test]
    fn decodes_chunks_extensions_and_trailers() {
        let wire = b"5;name=value\r\nhello\r\n6\r\n world\r\n0\r\nX-T: 1\r\n\r\nNEXT";
        let mut cur = Cursor::new(wire.to_vec());
        let mut out = Vec::new();
        ChunkedReader::new(&mut cur, 1000, 1000, None).read_to_end(&mut out).unwrap();
        assert_eq!(out, b"hello world");
        // stops right after the body: the next request starts there
        assert_eq!(&wire[cur.position() as usize..], b"NEXT");
        assert_eq!(decode(b"A\r\n0123456789\r\n0\r\n\r\n", 100).unwrap(), b"0123456789");
    }

    #[test]
    fn an_endless_size_line_stops_at_the_line_bound() {
        let wire = vec![b'1'; 1_000_000];
        let e = decode(&wire, usize::MAX).unwrap_err();
        assert_eq!(e.kind(), ErrorKind::InvalidData);
        let ext = [b"1;".to_vec(), vec![b'a'; 1_000_000]].concat();
        assert_eq!(decode(&ext, usize::MAX).unwrap_err().kind(), ErrorKind::InvalidData);
    }

    #[test]
    fn every_wire_byte_counts_against_the_limit() {
        // 200 one-byte chunks: 200 data bytes, 1200 on the wire
        let mut wire = Vec::new();
        for _ in 0..200 {
            wire.extend_from_slice(b"1\r\na\r\n");
        }
        wire.extend_from_slice(b"0\r\n\r\n");
        let e = decode(&wire, 1000).unwrap_err();
        assert!(e.get_ref().unwrap().is::<BodyTooLarge>());
        assert_eq!(decode(&wire, 2000).unwrap().len(), 200);
    }

    #[test]
    fn malformed_bodies_are_errors() {
        for wire in [
            &b"zz\r\nhello\r\n0\r\n\r\n"[..],
            b"5\nhello\r\n0\r\n\r\n",
            b"5\r\nhello!\r\n0\r\n\r\n",
            b"11111111111111111\r\n",
            b"5\r\nhel",
            b"\r\n",
        ] {
            assert!(decode(wire, 1000).is_err(), "{:?}", String::from_utf8_lossy(wire));
        }
        let mut long_trailers = b"0\r\n".to_vec();
        for _ in 0..100 {
            long_trailers.extend_from_slice(&[b"X: ".to_vec(), vec![b'a'; 200], b"\r\n".to_vec()].concat());
        }
        assert!(decode(&long_trailers, usize::MAX).is_err());
    }

    #[test]
    fn past_the_deadline_is_a_timeout() {
        let past = Instant::now() - Duration::from_millis(1);
        let mut r = ChunkedReader::new(Cursor::new(b"1;".to_vec()), 1000, 1000, Some(past));
        assert_eq!(r.read(&mut [0u8; 8]).unwrap_err().kind(), ErrorKind::TimedOut);
    }
}
