//! Timer/timeout utility implemented using `tokio::time::Sleep`.

use std::pin::Pin;

use crate::utils::SummersetError;

use tokio::time::{self, Sleep, Duration, Instant};

/// Timer utility for signalling after a given timeout. Supports reseting with
/// a different duration for implementing incremental backoff, etc.
///
/// Must be used within the context of a tokio runtime.
#[derive(Debug)]
pub struct Timer {
    /// Inner `tokio::time::Sleep` future, wrapped in a pinned box to support
    /// await multiple times.
    sleep: Pin<Box<Sleep>>,

    /// Timeout duration used by the last kick-off.
    last_dur: Duration,
}

impl Timer {
    /// Creates a new timer utility that immediately times-out after a
    /// zero-length duration.
    pub fn new() -> Self {
        Timer {
            sleep: Box::pin(time::sleep(Duration::ZERO)),
            last_dur: Duration::ZERO,
        }
    }

    /// Get the last timeout duration used.
    pub fn get_dur(&self) -> Duration {
        self.last_dur
    }

    /// Restarts the timer with the given duration.
    pub fn restart(&mut self, dur: Duration) -> Result<(), SummersetError> {
        if dur.is_zero() {
            return Err(SummersetError(format!(
                "invalid timeout duration {} ns",
                dur.as_nanos()
            )));
        }

        self.last_dur = dur;
        self.sleep.as_mut().reset(Instant::now() + dur);
        Ok(())
    }

    /// Waits for the timer to timeout. Typically, this should be used as a
    /// branch of a `tokio::select!`.
    pub async fn timeout(&mut self) {
        self.sleep.as_mut().await
    }
}

#[cfg(test)]
mod timer_tests {
    use super::*;
    use tokio::time::{Duration, Instant};

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn timer_new() {
        let timer = Timer::new();
        assert!(timer.get_dur().is_zero());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn timer_initial() -> Result<(), SummersetError> {
        let start = Instant::now();
        let mut timer = Timer::new();
        timer.timeout().await; // should complete immediately
        let finish = Instant::now();
        assert!(finish.duration_since(start) < Duration::from_millis(100));
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn timer_restart() -> Result<(), SummersetError> {
        let mut timer = Timer::new();
        // round 1 with 200ms timeout
        let mut start = Instant::now();
        timer.restart(Duration::from_millis(200))?;
        timer.timeout().await;
        let mut finish = Instant::now();
        assert!(finish.duration_since(start) >= Duration::from_millis(200));
        assert_eq!(timer.get_dur(), Duration::from_millis(200));
        // round 2 with 100ms incremental backoff
        start = Instant::now();
        timer.restart(timer.get_dur() + Duration::from_millis(100))?;
        timer.timeout().await;
        finish = Instant::now();
        assert!(finish.duration_since(start) >= Duration::from_millis(300));
        assert_eq!(timer.get_dur(), Duration::from_millis(300));
        Ok(())
    }
}
