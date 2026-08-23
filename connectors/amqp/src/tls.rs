//! Pure-Rust TLS for amqps:// (ADR-0026 Q1): rustls over amiquip's mio-0.6 stream.
//!
//! amiquip's `IoStream` is `Read + Write + mio::Evented + Send + 'static`, and its
//! own TLS path is native-tls (openssl-sys — C). We instead wrap a mio-0.6
//! `TcpStream` in a rustls `StreamOwned` and delegate `Evented` to the inner
//! socket: amiquip drives its event loop against a plain readable/writable stream,
//! but every byte is TLS. No C — the same transport-wrapping the MQTT connector
//! does (ADR-0025), plus the mio delegation. The TLS handshake runs lazily inside
//! rustls on the first reads/writes amiquip's loop performs, propagating WouldBlock
//! from the non-blocking socket so the loop re-polls until it completes.

use std::io::{self, BufReader, Read, Write};
use std::sync::Arc;

/// A rustls TLS stream over a mio-0.6 `TcpStream`, presented to amiquip as an
/// `IoStream`. `Evented` is delegated to the inner socket (rustls itself is not
/// pollable; the fd is).
pub struct RustlsMioStream {
    tls: rustls::StreamOwned<rustls::ClientConnection, mio::net::TcpStream>,
}

impl Read for RustlsMioStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.tls.read(buf)
    }
}
impl Write for RustlsMioStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.tls.write(buf)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.tls.flush()
    }
}
impl mio::Evented for RustlsMioStream {
    fn register(
        &self,
        poll: &mio::Poll,
        token: mio::Token,
        interest: mio::Ready,
        opts: mio::PollOpt,
    ) -> io::Result<()> {
        self.tls.sock.register(poll, token, interest, opts)
    }
    fn reregister(
        &self,
        poll: &mio::Poll,
        token: mio::Token,
        interest: mio::Ready,
        opts: mio::PollOpt,
    ) -> io::Result<()> {
        self.tls.sock.reregister(poll, token, interest, opts)
    }
    fn deregister(&self, poll: &mio::Poll) -> io::Result<()> {
        self.tls.sock.deregister(poll)
    }
}

/// Build the rustls root store: a caller-provided CA PEM (`VEJAS_AMQP_TLS_CA`, e.g.
/// a self-signed broker) when set, otherwise the bundled webpki roots.
fn root_store(ca_file: Option<&str>) -> Result<rustls::RootCertStore, String> {
    let mut roots = rustls::RootCertStore::empty();
    if let Some(ca) = ca_file {
        let f = std::fs::File::open(ca).map_err(|e| format!("open CA {ca}: {e}"))?;
        let certs = rustls_pemfile::certs(&mut BufReader::new(f))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| format!("parse CA {ca}: {e}"))?;
        let (added, _) = roots.add_parsable_certificates(certs);
        if added == 0 {
            return Err(format!("no usable certificates in {ca}"));
        }
    } else {
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    }
    Ok(roots)
}

/// Connect a TLS stream to `host:port`, verifying the server against the CA (or the
/// webpki roots) with `server_name` as the SNI / certificate name.
pub fn connect(
    host: &str,
    port: u16,
    server_name: &str,
    ca_file: Option<&str>,
) -> Result<RustlsMioStream, String> {
    let config = rustls::ClientConfig::builder()
        .with_root_certificates(root_store(ca_file)?)
        .with_no_client_auth();
    let name = rustls::pki_types::ServerName::try_from(server_name.to_string())
        .map_err(|_| format!("invalid TLS server name {server_name}"))?;
    let conn = rustls::ClientConnection::new(Arc::new(config), name)
        .map_err(|e| format!("rustls client init: {e}"))?;
    let addr = format!("{host}:{port}")
        .parse()
        .map_err(|e| format!("bad address {host}:{port}: {e}"))?;
    let sock = mio::net::TcpStream::connect(&addr).map_err(|e| format!("tcp connect: {e}"))?;
    Ok(RustlsMioStream {
        tls: rustls::StreamOwned::new(conn, sock),
    })
}
