//! A bounded pool of per-connection or per-request threads.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// One place in a bounded pool of per-connection or per-request threads, given
/// back when the thread ends, a panic included.
pub struct Slot(Arc<AtomicUsize>);

impl Slot {
    pub fn try_take(open: &Arc<AtomicUsize>, max: usize) -> Option<Slot> {
        open.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| (n < max).then_some(n + 1))
            .ok()
            .map(|_| Slot(open.clone()))
    }
}

impl Drop for Slot {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}
