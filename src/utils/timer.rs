//! Timer/timeout utility implemented using `tokio::time::Sleep` on a spawned
//! task and connecting it with the caller through `tokio::sync::watch` and
//! `tokio::sync::Notify` channels. This is suitable only for coarse-grained
//! timeout intervals.

use std::marker::Send;
use std::sync::Arc;

use crate::utils::SummersetError;

use futures::future::FutureExt;

use tokio::sync::{watch, Notify};
use tokio::time::{self, Duration, Instant};

/// Timer utility for signalling after a given timeout.
///
/// Supports reseting with a different duration for implementing incremental
/// backoff, etc. Must be used within the context of a tokio runtime.
#[derive(Debug)]
pub struct Timer {
    /// Deadline setting channel (caller side sender).
    deadline_tx: watch::Sender<Option<Instant>>,

    /// Timeout notification channel (caller side receiver).
    notify: Option<Arc<Notify>>,
}

impl Timer {
    /// Creates a new timer utility. If `use_notify` is true, will create a
    /// `sync::Notify` which gets notified once every timeout. If `explode_fn`
    /// is not `None`, this closure is called once every timeout.
    pub fn new<F>(use_notify: bool, explode_fn: Option<F>) -> Self
    where
        F: Fn() + Send + 'static,
    {
        let (deadline_tx, mut deadline_rx) = watch::channel(None);

        let notify = if use_notify {
            Some(Arc::new(Notify::new()))
        } else {
            None
        };
        let notify_ref = notify.clone();

        // spawn the background sleeper task
        tokio::spawn(async move {
            let sleep = time::sleep(Duration::ZERO);
            tokio::pin!(sleep);

            while deadline_rx.changed().await.is_ok() {
                // received a new deadline
                let deadline = *deadline_rx.borrow();
                if let Some(ddl) = deadline {
                    sleep.as_mut().reset(ddl);
                    (&mut sleep).await;

                    // explode only if deadline not changed since last wakeup
                    if let Ok(false) = deadline_rx.has_changed() {
                        if let Some(ref explode_fn) = explode_fn {
                            explode_fn();
                        }
                        if let Some(notify_ref) = notify_ref.as_ref() {
                            notify_ref.notify_one();
                        }
                    }
                }
            }
            // sender has been dropped, terminate
        });

        Timer {
            deadline_tx,
            notify,
        }
    }

    /// Kicks-off the timer with the given duration. Every call to `kickoff()`
    /// leads to one or zero permits inserted into `notify`; if a new call to
    /// `kickoff()` is made before the active one finishes, the timer restarts
    /// and will notify a timeout only at the new deadline.
    pub fn kickoff(&self, dur: Duration) -> Result<(), SummersetError> {
        if dur.is_zero() {
            return Err(SummersetError::msg(format!(
                "invalid timeout duration {} ns",
                dur.as_nanos()
            )));
        }

        self.deadline_tx.send(Some(Instant::now() + dur))?;
        Ok(())
    }

    /// Cancels the currently scheduled timeout if one is kicked-off or
    /// already ticked.
    pub fn cancel(&self) -> Result<(), SummersetError> {
        self.deadline_tx.send(None)?;

        // consume all existing timeout notifications
        if let Some(notify) = self.notify.as_ref() {
            while notify.notified().now_or_never().is_some() {}
        }

        Ok(())
    }

    /// Waits for a timeout notification. Typically, this should be used as a
    /// branch of a `tokio::select!`.
    pub async fn timeout(&self) {
        if let Some(notify) = self.notify.as_ref() {
            notify.notified().await;
        } else {
            panic!("timer `.timeout()` called but notify not in use")
        }
    }
}

impl Default for Timer {
    fn default() -> Self {
        // by default uses notify and has no explode function
        Self::new::<fn()>(true, None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;
    use tokio::time::{Duration, Instant};

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn timer_timeout() -> Result<(), SummersetError> {
        let timer = Arc::new(Timer::default());
        let timer_ref = timer.clone();
        let start = Instant::now();
        timer_ref.kickoff(Duration::from_millis(100))?;
        tokio::select! {
            () = timer.timeout() => {
                let finish = Instant::now();
                assert!(finish.duration_since(start) >= Duration::from_millis(100));
            }
        }
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn timer_restart() -> Result<(), SummersetError> {
        let timer = Arc::new(Timer::default());
        let timer_ref = timer.clone();
        let start = Instant::now();
        tokio::spawn(async move {
            // setter-side
            timer_ref.kickoff(Duration::from_millis(100))?;
            time::sleep(Duration::from_millis(50)).await;
            timer_ref.kickoff(Duration::from_millis(200))?;
            Ok::<(), SummersetError>(())
        });
        // looper-side
        tokio::select! {
            () = timer.timeout() => {
                let finish = Instant::now();
                assert!(finish.duration_since(start) >= Duration::from_millis(250));
            }
        }
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn timer_cancel() -> Result<(), SummersetError> {
        let timer = Arc::new(Timer::default());
        let timer_ref = timer.clone();
        let start = Instant::now();
        timer_ref.kickoff(Duration::from_millis(50))?;
        time::sleep(Duration::from_millis(100)).await;
        timer_ref.cancel()?;
        timer_ref.kickoff(Duration::from_millis(200))?;
        tokio::select! {
            () = timer.timeout() => {
                let finish = Instant::now();
                assert!(finish.duration_since(start) >= Duration::from_millis(300));
            }
        }
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn call_explode_fn() -> Result<(), SummersetError> {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let timer = Arc::new(Timer::new(
            true,
            Some(move || {
                tx.send(7).expect("explode_fn send should succeed");
            }),
        ));
        let timer_ref = timer.clone();
        // once
        let start = Instant::now();
        timer_ref.kickoff(Duration::from_millis(100))?;
        tokio::select! {
            () = timer.timeout() => {
                let finish = Instant::now();
                assert!(finish.duration_since(start) >= Duration::from_millis(100));
                assert_eq!(rx.recv().await, Some(7));
            }
        }
        // twice
        timer_ref.kickoff(Duration::from_millis(100))?;
        tokio::select! {
            () = timer.timeout() => {
                let finish = Instant::now();
                assert!(finish.duration_since(start) >= Duration::from_millis(200));
                assert_eq!(rx.recv().await, Some(7));
            }
        }
        Ok(())
    }
}
