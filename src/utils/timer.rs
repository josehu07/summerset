//! Timer/timeout utility implemented using `tokio::time::Sleep` on a spawned
//! task and connecting it with the caller through `tokio::sync::watch` and
//! `tokio::sync::Notify` channels. This is suitable only for coarse-grained
//! timeout intervals.

use std::marker::Send;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::utils::SummersetError;

use futures::future::FutureExt;

use tokio::sync::{watch, Notify};
use tokio::time::{self, Duration, Instant};

/// Timer utility for signalling after a given timeout.
///
/// Supports resetting with a different duration for implementing incremental
/// backoff, etc. Must be used within the context of a tokio runtime.
#[derive(Debug)]
pub struct Timer {
    /// Deadline setting channel (caller side sender).
    deadline_tx: watch::Sender<Option<Instant>>,

    /// Timeout notification channel (caller side receiver).
    notify: Option<Arc<Notify>>,

    /// True if the timer has exploded since last kickoff; false otherwise.
    exploded: Arc<AtomicBool>,
}

impl Timer {
    /// Creates a new timer utility. If `use_notify` is true, will create a
    /// `sync::Notify` which gets notified once every timeout. If `explode_fn`
    /// is not `None`, this closure is called once every timeout.
    /// If `allow_backwards` is true, the timer will allow kicking off to a
    /// deadline that's earlier than previously kicked off deadline (doing
    /// so adds a slight `tokio::select!` overhead).
    pub fn new<F>(
        use_notify: bool,
        explode_fn: Option<F>,
        allow_backwards: bool,
    ) -> Self
    where
        F: Fn() + Send + 'static,
    {
        let (deadline_tx, deadline_rx) = watch::channel(None);

        let notify = if use_notify {
            Some(Arc::new(Notify::new()))
        } else {
            None
        };
        let exploded = Arc::new(AtomicBool::new(false));

        // spawn the background sleeper task
        let mut sleeper = TimerSleeperTask::new(
            deadline_rx,
            notify.clone(),
            explode_fn,
            exploded.clone(),
            allow_backwards,
        );
        tokio::spawn(async move { sleeper.run().await });

        Timer {
            deadline_tx,
            notify,
            exploded,
        }
    }

    /// Kicks-off the timer with the given duration. Every call to `kickoff()`
    /// leads to one or zero permits inserted into `notify`; if a new call to
    /// `kickoff()` is made before the active one finishes, the timer restarts
    /// and will notify a timeout only at the new deadline.
    pub fn kickoff(&self, dur: Duration) -> Result<(), SummersetError> {
        if dur.is_zero() {
            return Err(SummersetError::msg(format!(
                "invalid timeout duration {:?}",
                dur
            )));
        }

        self.deadline_tx.send(Some(Instant::now() + dur))?;

        self.exploded.store(false, Ordering::Release);
        Ok(())
    }

    /// Extends the timer with the given duration beyond current deadline (or
    /// beyond now if current deadline already in the past). Behaves similarly
    /// to `kickoff()`.
    pub fn extend(&self, dur: Duration) -> Result<(), SummersetError> {
        if dur.is_zero() {
            return Err(SummersetError::msg(format!(
                "invalid timeout duration {:?}",
                dur
            )));
        }

        self.deadline_tx.send_modify(|ddl| {
            if let Some(ddl) = ddl.as_mut() {
                if *ddl < Instant::now() {
                    *ddl = Instant::now();
                }
                *ddl += dur;
            } else {
                *ddl = Some(Instant::now() + dur);
            }
        });

        self.exploded.store(false, Ordering::Release);
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

        self.exploded.store(false, Ordering::Release);
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

    /// Checks if the timer has exploded since last kickoff.
    pub fn exploded(&self) -> bool {
        self.exploded.load(Ordering::Acquire)
    }
}

impl Default for Timer {
    fn default() -> Self {
        // by default uses notify and has no explode function
        Self::new::<fn()>(true, None, false)
    }
}

/// Timer background sleeper task.
struct TimerSleeperTask<F> {
    deadline_rx: watch::Receiver<Option<Instant>>,

    /// If not `None`, this semaphore will be notified once upon every timeout.
    notify: Option<Arc<Notify>>,
    /// If not `None`, the function will be called upon every timeout.
    explode_fn: Option<F>,
    /// Indicates whether the timer has exploded since last kickoff.
    exploded: Arc<AtomicBool>,

    allow_backwards: bool,
}

impl<F> TimerSleeperTask<F>
where
    F: Fn() + Send + 'static,
{
    /// Creates the background sleeper task associated with this timer.
    fn new(
        deadline_rx: watch::Receiver<Option<Instant>>,
        notify: Option<Arc<Notify>>,
        explode_fn: Option<F>,
        exploded: Arc<AtomicBool>,
        allow_backwards: bool,
    ) -> Self {
        TimerSleeperTask {
            deadline_rx,
            notify,
            explode_fn,
            exploded,
            allow_backwards,
        }
    }

    /// Starts the background sleeper task.
    async fn run(&mut self) {
        let sleep = time::sleep(Duration::ZERO);
        tokio::pin!(sleep);

        while self.deadline_rx.changed().await.is_ok() {
            // received a new deadline
            let deadline = *self.deadline_rx.borrow();
            if let Some(ddl) = deadline {
                sleep.as_mut().reset(ddl);

                if !self.allow_backwards {
                    (&mut sleep).await;
                } else {
                    tokio::select! {
                        () = (&mut sleep) => {
                            // deadline reached and no newer deadline set
                        }
                        res = self.deadline_rx.changed() => {
                            // newer deadline set, try to use that instead
                            if res.is_err() {
                                break;
                            }
                            self.deadline_rx.mark_changed();
                            continue;
                        }
                    }
                }

                // explode only if deadline not changed since last wakeup
                if let Ok(false) = self.deadline_rx.has_changed() {
                    self.exploded.store(true, Ordering::Release);
                    if let Some(ref explode_fn) = self.explode_fn {
                        explode_fn();
                    }
                    if let Some(notify) = self.notify.as_ref() {
                        notify.notify_one();
                    }
                }
            }
        }

        // sender has been dropped, terminate
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
        // once
        timer_ref.kickoff(Duration::from_millis(300))?;
        assert!(!timer.exploded());
        tokio::select! {
            () = timer.timeout() => {
                let finish = Instant::now();
                assert!(finish.duration_since(start) >= Duration::from_millis(300));
                assert!(timer.exploded());
            }
        }
        // twice
        timer_ref.kickoff(Duration::from_millis(300))?;
        assert!(!timer.exploded());
        tokio::select! {
            () = timer.timeout() => {
                let finish = Instant::now();
                assert!(finish.duration_since(start) >= Duration::from_millis(600));
                assert!(timer.exploded());
            }
        }
        // extend
        timer_ref.kickoff(Duration::from_millis(500))?;
        assert!(!timer.exploded());
        time::sleep(Duration::from_millis(200)).await;
        timer_ref.extend(Duration::from_millis(500))?;
        tokio::select! {
            () = timer.timeout() => {
                let finish = Instant::now();
                assert!(finish.duration_since(start) >= Duration::from_millis(1300));
                assert!(timer.exploded());
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
            timer_ref.kickoff(Duration::from_millis(400))?;
            time::sleep(Duration::from_millis(100)).await;
            timer_ref.kickoff(Duration::from_millis(400))?;
            Ok::<(), SummersetError>(())
        });
        // looper-side
        tokio::select! {
            () = timer.timeout() => {
                let finish = Instant::now();
                assert!(finish.duration_since(start) >= Duration::from_millis(500));
                assert!(finish.duration_since(start) < Duration::from_millis(800));
            }
        }
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn timer_cancel() -> Result<(), SummersetError> {
        let timer = Arc::new(Timer::default());
        let timer_ref = timer.clone();
        let start = Instant::now();
        timer_ref.kickoff(Duration::from_millis(300))?;
        time::sleep(Duration::from_millis(100)).await;
        timer_ref.cancel()?;
        assert!(!timer.exploded());
        timer_ref.kickoff(Duration::from_millis(500))?;
        tokio::select! {
            () = timer.timeout() => {
                let finish = Instant::now();
                assert!(finish.duration_since(start) >= Duration::from_millis(600));
                assert!(timer.exploded());
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
            false,
        ));
        let timer_ref = timer.clone();
        // once
        let start = Instant::now();
        timer_ref.kickoff(Duration::from_millis(300))?;
        assert!(!timer.exploded());
        tokio::select! {
            () = timer.timeout() => {
                let finish = Instant::now();
                assert!(finish.duration_since(start) >= Duration::from_millis(300));
                assert_eq!(rx.recv().await, Some(7));
                assert!(timer.exploded());
            }
        }
        // twice
        timer_ref.kickoff(Duration::from_millis(300))?;
        assert!(!timer.exploded());
        tokio::select! {
            () = timer.timeout() => {
                let finish = Instant::now();
                assert!(finish.duration_since(start) >= Duration::from_millis(600));
                assert_eq!(rx.recv().await, Some(7));
                assert!(timer.exploded());
            }
        }
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn set_backwards() -> Result<(), SummersetError> {
        let timer = Arc::new(Timer::new::<fn()>(true, None, true));
        let timer_ref = timer.clone();
        let start = Instant::now();
        // set long
        timer_ref.kickoff(Duration::from_millis(600))?;
        assert!(!timer.exploded());
        time::sleep(Duration::from_millis(100)).await;
        // set short
        timer_ref.kickoff(Duration::from_millis(200))?;
        assert!(!timer.exploded());
        tokio::select! {
            () = timer.timeout() => {
                let finish = Instant::now();
                assert!(finish.duration_since(start) >= Duration::from_millis(300));
                assert!(finish.duration_since(start) < Duration::from_millis(600));
                assert!(timer.exploded());
            }
        }
        Ok(())
    }
}
