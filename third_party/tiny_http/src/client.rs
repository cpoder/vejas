use ascii::AsciiString;

use std::io::Error as IoError;
use std::io::Result as IoResult;
use std::io::{BufReader, BufWriter, ErrorKind, Read};

use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::mpsc::Receiver;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::common::{HTTPVersion, Method};
use crate::request::BodyLimits;
use crate::util::{Abandon, Clock, RefinedTcpStream};
use crate::util::{SequentialReader, SequentialReaderBuilder, SequentialWriterBuilder};
use crate::{Limits, Request};

/// Vejas patch: the longest request line or header line, in bytes.
const MAX_LINE: usize = 8 * 1024;
/// Vejas patch: the most headers one request may carry.
const MAX_HEADERS: usize = 100;
/// Vejas patch: after an early refusal (413, 400...), what is read and thrown
/// away before the connection closes, so the answer is not lost to a reset
/// while the client is still sending: this many bytes, for this long at most.
const LINGER_BYTES: usize = 64 * 1024;
const LINGER_TIME: Duration = Duration::from_secs(2);
const LINGER_PAUSE: Duration = Duration::from_millis(250);

/// A ClientConnection is an object that will store a socket to a client
/// and return Request objects.
pub struct ClientConnection {
    // address of the client
    remote_addr: IoResult<Option<SocketAddr>>,

    // sequence of Readers to the stream, so that the data is not read in
    //  the wrong order
    source: SequentialReaderBuilder<BufReader<RefinedTcpStream>>,

    // sequence of Writers to the stream, to avoid writing response #2 before
    //  response #1
    sink: SequentialWriterBuilder<BufWriter<RefinedTcpStream>>,

    // Reader to read the next header from
    next_header_source: SequentialReader<BufReader<RefinedTcpStream>>,

    // set to true if we know that the previous request is the last one
    no_more_requests: bool,

    // true if the connection goes through SSL
    secure: bool,

    // Vejas patch: the bounds this connection is held to, and the flag a
    // request sets when it leaves its body unread.
    limits: Limits,
    abandon: Abandon,
    // Vejas patch: the deadlines of this connection's socket, and the signal
    // that the last response was written (no head is read before it is).
    clock: Option<Arc<Clock>>,
    prev_response: Option<Receiver<()>>,
}

/// Error that can happen when reading a request.
#[derive(Debug)]
enum ReadError {
    WrongRequestLine,
    WrongHeader(HTTPVersion),
    /// the client sent an unrecognized `Expect` header
    ExpectationFailed(HTTPVersion),
    ReadIoError(IoError),
    /// Vejas patch: a line or a header count past the bound (431)
    HeadersTooLarge,
    /// Vejas patch: a declared body past the bound (413)
    BodyTooLarge(HTTPVersion),
    /// Vejas patch: a Content-Length that is not one number, a Content-Length
    /// with a Transfer-Encoding, or a line that does not end in CRLF (400)
    BadFraming(HTTPVersion),
    /// Vejas patch: a Transfer-Encoding other than chunked (501)
    NotImplemented(HTTPVersion),
}

impl ClientConnection {
    /// Creates a new `ClientConnection` that takes ownership of the `TcpStream`.
    pub fn new(
        write_socket: RefinedTcpStream,
        mut read_socket: RefinedTcpStream,
        limits: Limits,
    ) -> ClientConnection {
        let remote_addr = read_socket.peer_addr();
        let secure = read_socket.secure();
        let clock = read_socket.clock();

        let mut source = SequentialReaderBuilder::new(BufReader::with_capacity(1024, read_socket));
        let first_header = source.next().unwrap();

        ClientConnection {
            source,
            sink: SequentialWriterBuilder::new(BufWriter::with_capacity(1024, write_socket)),
            remote_addr,
            next_header_source: first_header,
            no_more_requests: false,
            secure,
            limits,
            abandon: Abandon::new(),
            clock,
            prev_response: None,
        }
    }

    /// Vejas patch: reads from now on are due within `d` (the timeout when
    /// `None`); the deadline set, also when the socket has no clock.
    fn read_within(&self, d: Option<Duration>) -> Option<Instant> {
        match &self.clock {
            Some(clock) => clock.read_within(d),
            None => d
                .or(self.limits.timeout)
                .and_then(|d| Instant::now().checked_add(d)),
        }
    }

    /// Vejas patch: answers `code`, then reads and throws away what the client
    /// may still be sending, a little and briefly, and ends the connection.
    fn refuse(&mut self, code: u16, version: HTTPVersion, linger: bool) -> Option<Request> {
        use crate::{Response, StatusCode};
        use std::io::Write;
        let mut writer = self.sink.next().unwrap();
        let response = Response::new_empty(StatusCode(code));
        if response.raw_print(&mut writer, version, &[], false, None).is_ok()
            && writer.flush().is_ok()
            && linger
        {
            let short = self.limits.timeout.map_or(LINGER_TIME, |t| t.min(LINGER_TIME));
            let by = Instant::now() + short;
            let mut chunk = [0u8; 4096];
            let mut thrown = 0;
            while thrown < LINGER_BYTES {
                // ends as soon as the client pauses, or at `by`
                let left = match by.checked_duration_since(Instant::now()) {
                    Some(left) if !left.is_zero() => left,
                    _ => break,
                };
                self.read_within(Some(left.min(LINGER_PAUSE)));
                match self.next_header_source.read(&mut chunk) {
                    Ok(0) | Err(_) => break,
                    Ok(n) => thrown += n,
                }
            }
        }
        None
    }

    /// true if the connection is HTTPS
    pub fn secure(&self) -> bool {
        self.secure
    }

    /// Reads the next line from self.next_header_source.
    ///
    /// Reads until `CRLF` is reached. The next read will start
    ///  at the first byte of the new line.
    fn read_next_line(
        &mut self,
        deadline: &mut Option<Instant>,
        first: bool,
    ) -> Result<AsciiString, ReadError> {
        let mut buf = Vec::new();
        let mut prev_byte_was_cr = false;

        loop {
            // Vejas patch: a line has a length and a time it must come in.
            if buf.len() > MAX_LINE {
                return Err(ReadError::HeadersTooLarge);
            }
            if deadline.map_or(false, |d| Instant::now() >= d) {
                let late = IoError::new(ErrorKind::TimedOut, "request head too slow");
                return Err(ReadError::ReadIoError(late));
            }
            let byte = self.next_header_source.by_ref().bytes().next();

            let byte = match byte {
                Some(b) => b.map_err(ReadError::ReadIoError)?,
                None => {
                    let eof = IoError::new(ErrorKind::ConnectionAborted, "Unexpected EOF");
                    return Err(ReadError::ReadIoError(eof));
                }
            };

            // Vejas patch: the first byte of a request starts its clock: the
            // head and the body are due within the timeout from here.
            if first && buf.is_empty() {
                *deadline = self.read_within(None);
            }

            // Vejas patch: only CRLF ends a line.
            if byte == b'\n' && !prev_byte_was_cr {
                return Err(ReadError::BadFraming(HTTPVersion(1, 1)));
            }

            if byte == b'\n' && prev_byte_was_cr {
                buf.pop(); // removing the '\r'
                return AsciiString::from_ascii(buf).map_err(|_| {
                    let e = IoError::new(ErrorKind::InvalidInput, "Header is not in ASCII");
                    ReadError::ReadIoError(e)
                });
            }

            prev_byte_was_cr = byte == b'\r';

            buf.push(byte);
        }
    }

    /// Reads a request from the stream.
    /// Blocks until the header has been read.
    fn read(&mut self) -> Result<Request, ReadError> {
        // Vejas patch: no head is read before the previous response on this
        // connection is written. Without it, a client that pipelines and never
        // reads had requests parsed ahead with no bound, each holding a
        // handler that waited on the one before to write.
        if let Some(written) = self.prev_response.take() {
            written.recv().ok();
        }
        // Vejas patch: a request before this one that left its body unread
        // left the stream mid-body. What comes next is not a request.
        self.next_header_source.wait_turn();
        if self.abandon.is_set() {
            let e = IoError::new(ErrorKind::ConnectionAborted, "previous body left unread");
            return Err(ReadError::ReadIoError(e));
        }
        // idle until the first byte, then the head and the body: each the timeout
        let mut deadline = self.read_within(None);
        let (method, path, version, headers) = {
            // reading the request line
            let (method, path, version) = {
                let line = self.read_next_line(&mut deadline, true)?;

                parse_request_line(
                    line.as_str().trim(), // TODO: remove this conversion
                )?
            };

            // getting all headers
            let headers = {
                let mut headers: Vec<crate::Header> = Vec::new();
                loop {
                    let line = self.read_next_line(&mut deadline, false)?;

                    if line.is_empty() {
                        break;
                    };
                    if headers.len() >= MAX_HEADERS {
                        return Err(ReadError::HeadersTooLarge);
                    }
                    headers.push(match FromStr::from_str(line.as_str().trim()) {
                        // TODO: remove this conversion
                        Ok(h) => h,
                        _ => return Err(ReadError::WrongHeader(version)),
                    });
                }

                headers
            };

            (method, path, version, headers)
        };

        // Vejas patch: a body is framed one way. Transfer-Encoding is chunked
        // (identity before it is allowed) or it is not served; with it, a
        // Content-Length is an error, not something to ignore.
        let codings: Vec<String> = headers
            .iter()
            .filter(|h| h.field.equiv("Transfer-Encoding"))
            .flat_map(|h| h.value.as_str().split(',').map(|c| c.trim().to_ascii_lowercase()).collect::<Vec<_>>())
            .collect();
        if !codings.is_empty() {
            let (last, before) = codings.split_last().unwrap();
            if last != "chunked" || before.iter().any(|c| c != "identity") {
                return Err(ReadError::NotImplemented(version));
            }
            if headers.iter().any(|h| h.field.equiv("Content-Length")) {
                return Err(ReadError::BadFraming(version));
            }
        }
        // Vejas patch: the declared length is one number, the same in every
        // Content-Length, and within the bound, or the body is not read at all.
        if codings.is_empty() {
            let mut declared: Option<usize> = None;
            for h in headers.iter().filter(|h| h.field.equiv("Content-Length")) {
                let v = h.value.as_str();
                let n = match v.parse::<usize>() {
                    Ok(n) if v.bytes().all(|b| b.is_ascii_digit()) => n,
                    _ => return Err(ReadError::BadFraming(version)),
                };
                if declared.map_or(false, |d| d != n) {
                    return Err(ReadError::BadFraming(version));
                }
                declared = Some(n);
            }
            if declared.map_or(false, |n| n > self.limits.max_body_bytes) {
                return Err(ReadError::BodyTooLarge(version));
            }
        }

        // building the writer for the request; told when it is written
        let (writer, written) = self.sink.next_watched();
        self.prev_response = Some(written);

        // follow-up for next potential request
        let mut data_source = self.source.next().unwrap();
        std::mem::swap(&mut self.next_header_source, &mut data_source);

        // building the next reader
        let request = crate::request::new_request(
            self.secure,
            method,
            path,
            version.clone(),
            headers,
            *self.remote_addr.as_ref().unwrap(),
            data_source,
            writer,
            BodyLimits {
                max: self.limits.max_body_bytes,
                // the same deadline as the head: one request, one timeout
                deadline,
                abandon: self.abandon.clone(),
            },
        )
        .map_err(|e| {
            use crate::request;
            match e {
                request::RequestCreationError::CreationIoError(e) => ReadError::ReadIoError(e),
                request::RequestCreationError::ExpectationFailed => {
                    ReadError::ExpectationFailed(version)
                }
            }
        })?;

        // return the request
        Ok(request)
    }
}

impl Iterator for ClientConnection {
    type Item = Request;

    /// Blocks until the next Request is available.
    /// Returns None when no new Requests will come from the client.
    fn next(&mut self) -> Option<Request> {
        use crate::{Response, StatusCode};

        // the client sent a "connection: close" header in this previous request
        //  or is using HTTP 1.0, meaning that no new request will come
        if self.no_more_requests {
            return None;
        }

        loop {
            let rq = match self.read() {
                Err(ReadError::WrongRequestLine) => {
                    let writer = self.sink.next().unwrap();
                    let response = Response::new_empty(StatusCode(400));
                    response
                        .raw_print(writer, HTTPVersion(1, 1), &[], false, None)
                        .ok();
                    return None; // we don't know where the next request would start,
                                 // se we have to close
                }

                Err(ReadError::WrongHeader(ver)) => {
                    let writer = self.sink.next().unwrap();
                    let response = Response::new_empty(StatusCode(400));
                    response.raw_print(writer, ver, &[], false, None).ok();
                    return None; // we don't know where the next request would start,
                                 // se we have to close
                }

                Err(ReadError::ReadIoError(ref err)) if err.kind() == ErrorKind::TimedOut => {
                    // request timeout
                    let writer = self.sink.next().unwrap();
                    let response = Response::new_empty(StatusCode(408));
                    response
                        .raw_print(writer, HTTPVersion(1, 1), &[], false, None)
                        .ok();
                    return None; // closing the connection
                }

                Err(ReadError::ExpectationFailed(ver)) => {
                    let writer = self.sink.next().unwrap();
                    let response = Response::new_empty(StatusCode(417));
                    response.raw_print(writer, ver, &[], true, None).ok();
                    return None; // TODO: should be recoverable, but needs handling in case of body
                }

                Err(ReadError::ReadIoError(_)) => return None,

                // Vejas patch: answered, then closed. The body (or the rest of
                // the head) is not read as a request, so where a next request
                // would start is not known. What the client is still sending
                // is read a little first, so the answer is not lost to a reset.
                Err(ReadError::HeadersTooLarge) => {
                    return self.refuse(431, HTTPVersion(1, 1), true);
                }

                Err(ReadError::BodyTooLarge(ver)) => return self.refuse(413, ver, true),

                Err(ReadError::BadFraming(ver)) => return self.refuse(400, ver, true),

                Err(ReadError::NotImplemented(ver)) => return self.refuse(501, ver, true),

                Ok(rq) => rq,
            };

            // checking HTTP version
            if *rq.http_version() > (1, 1) {
                let writer = self.sink.next().unwrap();
                let response = Response::from_string(
                    "This server only supports HTTP versions 1.0 and 1.1".to_owned(),
                )
                .with_status_code(StatusCode(505));
                response
                    .raw_print(writer, HTTPVersion(1, 1), &[], false, None)
                    .ok();
                continue;
            }

            // updating the status of the connection
            let connection_header = rq
                .headers()
                .iter()
                .find(|h| h.field.equiv("Connection"))
                .map(|h| h.value.as_str());

            let lowercase = connection_header.map(|h| h.to_ascii_lowercase());

            match lowercase {
                Some(ref val) if val.contains("close") => self.no_more_requests = true,
                Some(ref val) if val.contains("upgrade") => self.no_more_requests = true,
                Some(ref val)
                    if !val.contains("keep-alive") && *rq.http_version() == HTTPVersion(1, 0) =>
                {
                    self.no_more_requests = true
                }
                None if *rq.http_version() == HTTPVersion(1, 0) => self.no_more_requests = true,
                _ => (),
            };

            // returning the request
            return Some(rq);
        }
    }
}

/// Parses a "HTTP/1.1" string.
fn parse_http_version(version: &str) -> Result<HTTPVersion, ReadError> {
    let (major, minor) = match version {
        "HTTP/0.9" => (0, 9),
        "HTTP/1.0" => (1, 0),
        "HTTP/1.1" => (1, 1),
        "HTTP/2.0" => (2, 0),
        "HTTP/3.0" => (3, 0),
        _ => return Err(ReadError::WrongRequestLine),
    };

    Ok(HTTPVersion(major, minor))
}

/// Parses the request line of the request.
/// eg. GET / HTTP/1.1
fn parse_request_line(line: &str) -> Result<(Method, String, HTTPVersion), ReadError> {
    let mut parts = line.split(' ');

    let method = parts.next().and_then(|w| w.parse().ok());
    let path = parts.next().map(ToOwned::to_owned);
    let version = parts.next().and_then(|w| parse_http_version(w).ok());

    method
        .and_then(|method| Some((method, path?, version?)))
        .ok_or(ReadError::WrongRequestLine)
}

#[cfg(test)]
mod test {
    #[test]
    fn test_parse_request_line() {
        let (method, path, ver) = super::parse_request_line("GET /hello HTTP/1.1").unwrap();

        assert!(method == crate::Method::Get);
        assert!(path == "/hello");
        assert!(ver == crate::common::HTTPVersion(1, 1));

        assert!(super::parse_request_line("GET /hello").is_err());
        assert!(super::parse_request_line("qsd qsd qsd").is_err());
    }
}
