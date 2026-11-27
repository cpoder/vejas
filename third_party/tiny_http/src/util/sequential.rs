use std::io::Result as IoResult;
use std::io::{Read, Write};

use std::sync::mpsc::channel;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, Mutex};

use std::mem;
use std::sync::mpsc::RecvTimeoutError;
use std::time::Duration;

pub struct SequentialReaderBuilder<R>
where
    R: Read + Send,
{
    inner: SequentialReaderBuilderInner<R>,
}

enum SequentialReaderBuilderInner<R>
where
    R: Read + Send,
{
    First(R),
    NotFirst(Receiver<R>),
}

pub struct SequentialReader<R>
where
    R: Read + Send,
{
    inner: SequentialReaderInner<R>,
    next: Sender<R>,
}

enum SequentialReaderInner<R>
where
    R: Read + Send,
{
    MyTurn(R),
    Waiting(Receiver<R>),
    Empty,
}

pub struct SequentialWriterBuilder<W>
where
    W: Write + Send,
{
    writer: Arc<Mutex<W>>,
    next_trigger: Option<Receiver<()>>,
    // Vejas patch: how long a writer waits for the one before it
    wait: Option<Duration>,
}

pub struct SequentialWriter<W>
where
    W: Write + Send,
{
    trigger: Option<Receiver<()>>,
    writer: Arc<Mutex<W>>,
    on_finish: Sender<()>,
    // Vejas patch: told when this writer is done (see `next_watched`)
    watcher: Option<Sender<()>>,
    wait: Option<Duration>,
}

impl<R: Read + Send> SequentialReaderBuilder<R> {
    pub fn new(reader: R) -> SequentialReaderBuilder<R> {
        SequentialReaderBuilder {
            inner: SequentialReaderBuilderInner::First(reader),
        }
    }
}

impl<W: Write + Send> SequentialWriterBuilder<W> {
    pub fn new(writer: W) -> SequentialWriterBuilder<W> {
        SequentialWriterBuilder {
            writer: Arc::new(Mutex::new(writer)),
            next_trigger: None,
            wait: None,
        }
    }

    /// Vejas patch: a writer waits at most `wait` for the one before it to be
    /// done, then fails its write with `TimedOut` rather than wait for good.
    /// A backstop: the connection reads no head before the last response is
    /// written, so a writer has no one to wait for in the normal course.
    pub fn with_wait(mut self, wait: Option<Duration>) -> Self {
        self.wait = wait;
        self
    }
}

impl<R: Read + Send> Iterator for SequentialReaderBuilder<R> {
    type Item = SequentialReader<R>;

    fn next(&mut self) -> Option<SequentialReader<R>> {
        let (tx, rx) = channel();

        let inner = mem::replace(&mut self.inner, SequentialReaderBuilderInner::NotFirst(rx));

        match inner {
            SequentialReaderBuilderInner::First(reader) => Some(SequentialReader {
                inner: SequentialReaderInner::MyTurn(reader),
                next: tx,
            }),

            SequentialReaderBuilderInner::NotFirst(previous) => Some(SequentialReader {
                inner: SequentialReaderInner::Waiting(previous),
                next: tx,
            }),
        }
    }
}

impl<W: Write + Send> Iterator for SequentialWriterBuilder<W> {
    type Item = SequentialWriter<W>;
    fn next(&mut self) -> Option<SequentialWriter<W>> {
        let (tx, rx) = channel();
        let mut next_next_trigger = Some(rx);
        ::std::mem::swap(&mut next_next_trigger, &mut self.next_trigger);

        Some(SequentialWriter {
            trigger: next_next_trigger,
            writer: self.writer.clone(),
            on_finish: tx,
            watcher: None,
            wait: self.wait,
        })
    }
}

impl<W: Write + Send> SequentialWriterBuilder<W> {
    /// Vejas patch: the next writer, and a receiver told when it is dropped,
    /// that is when the response it carries is written.
    pub fn next_watched(&mut self) -> (SequentialWriter<W>, Receiver<()>) {
        let (tx, rx) = channel();
        let mut writer = self.next().unwrap();
        writer.watcher = Some(tx);
        (writer, rx)
    }
}

impl<R: Read + Send> SequentialReader<R> {
    /// Vejas patch: blocks until the readers before this one are done, without
    /// reading, so the connection can look at what they left behind first.
    pub fn wait_turn(&mut self) {
        if let SequentialReaderInner::Waiting(ref mut recv) = self.inner {
            let reader = recv.recv().unwrap();
            self.inner = SequentialReaderInner::MyTurn(reader);
        }
    }
}

impl<R: Read + Send> Read for SequentialReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
        let mut reader = match self.inner {
            SequentialReaderInner::MyTurn(ref mut reader) => return reader.read(buf),
            SequentialReaderInner::Waiting(ref mut recv) => recv.recv().unwrap(),
            SequentialReaderInner::Empty => unreachable!(),
        };

        let result = reader.read(buf);
        self.inner = SequentialReaderInner::MyTurn(reader);
        result
    }
}

impl<W: Write + Send> SequentialWriter<W> {
    /// Vejas patch: waits for the writer before this one, at most `wait`.
    fn wait_turn(&mut self) -> IoResult<()> {
        if let Some(v) = self.trigger.as_mut() {
            let waited = match self.wait {
                Some(wait) => v.recv_timeout(wait),
                None => v.recv().map_err(|_| RecvTimeoutError::Disconnected),
            };
            match waited {
                // the writer before is done (or gone): this one's turn
                Ok(()) | Err(RecvTimeoutError::Disconnected) => {}
                Err(RecvTimeoutError::Timeout) => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "the response before this one was never written",
                    ));
                }
            }
        }
        self.trigger = None;
        Ok(())
    }
}

impl<W: Write + Send> Write for SequentialWriter<W> {
    fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
        self.wait_turn()?;
        self.writer.lock().unwrap().write(buf)
    }

    fn flush(&mut self) -> IoResult<()> {
        self.wait_turn()?;
        self.writer.lock().unwrap().flush()
    }
}

impl<R> Drop for SequentialReader<R>
where
    R: Read + Send,
{
    fn drop(&mut self) {
        let inner = mem::replace(&mut self.inner, SequentialReaderInner::Empty);

        match inner {
            SequentialReaderInner::MyTurn(reader) => {
                self.next.send(reader).ok();
            }
            SequentialReaderInner::Waiting(recv) => {
                let reader = recv.recv().unwrap();
                self.next.send(reader).ok();
            }
            SequentialReaderInner::Empty => (),
        }
    }
}

impl<W> Drop for SequentialWriter<W>
where
    W: Write + Send,
{
    fn drop(&mut self) {
        self.on_finish.send(()).ok();
        if let Some(watcher) = self.watcher.take() {
            watcher.send(()).ok();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::SequentialWriterBuilder;
    use std::io::{ErrorKind, Write};
    use std::time::Duration;

    #[test]
    fn a_writer_waits_for_the_one_before_at_most_its_wait() {
        let mut b = SequentialWriterBuilder::new(Vec::new()).with_wait(Some(Duration::from_millis(50)));
        let first = b.next().unwrap();
        let mut second = b.next().unwrap();
        assert_eq!(second.write(b"x").unwrap_err().kind(), ErrorKind::TimedOut);
        drop(first);
        let mut third = b.next().unwrap();
        drop(second);
        assert!(third.write(b"y").is_ok(), "the turn passes on");
    }
}
