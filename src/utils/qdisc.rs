//! Helpers for running `tc qdisc` commands.

use std::fmt;
use std::process::Command;

use crate::utils::SummersetError;

static DEV_PATTERN: &str = "veths";

/// Helper struct holding qdisc information.
pub struct QdiscInfo {
    /// Delay in ms.
    pub delay: f64,

    /// Jitter in ms.
    pub jitter: f64,

    /// Rate in Gbps.
    pub rate: f64,
}

impl fmt::Display for QdiscInfo {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{{{:.0} +{:.0}, {:.1}}}",
            self.delay, self.jitter, self.rate
        )
    }
}

impl QdiscInfo {
    /// Query `tc qdisc` info by running the command. Returns the output line
    /// with expected device.
    fn run_qdisc_show() -> Result<String, SummersetError> {
        let output = String::from_utf8(
            Command::new("tc").arg("qdisc").arg("show").output()?.stdout,
        )?;
        for line in output.lines() {
            if line.contains("dev")
                && line.contains(DEV_PATTERN)
                && line.contains("root")
            {
                return Ok(line.trim().to_string());
            }
        }
        Err(SummersetError(
            "error getting `tc qdisc show` output line".into(),
        ))
    }

    /// Parse time field into float ms.
    #[inline]
    fn parse_time_ms(seg: &str) -> Result<f64, SummersetError> {
        let (multiplier, tail) = if seg.ends_with("us") {
            (0.001, 2)
        } else if seg.ends_with("ms") {
            (1.0, 2)
        } else if seg.ends_with('s') {
            (1000.0, 1)
        } else {
            (0.001, 0) // no unit means usecs
        };
        let num = seg[..seg.len() - tail].parse::<f64>()?;
        Ok(num * multiplier)
    }

    /// Parse rate field into float Gbps.
    #[inline]
    fn parse_rate_gbps(seg: &str) -> Result<f64, SummersetError> {
        const GBIT: f64 = 1024.0 * 1024.0 * 1024.0;
        let (multiplier, tail) = if seg.ends_with("kbit") {
            (1000.0 / GBIT, 4)
        } else if seg.ends_with("kibit") {
            (1024.0 / GBIT, 5)
        } else if seg.ends_with("Kbit") {
            (1024.0 / GBIT, 4)
        } else if seg.ends_with("mbit") {
            (1_000_000.0 / GBIT, 4)
        } else if seg.ends_with("mibit") {
            (1024.0 * 1024.0 / GBIT, 5)
        } else if seg.ends_with("Mbit") {
            (1024.0 * 1024.0 / GBIT, 4)
        } else if seg.ends_with("gbit") {
            (1_000_000_000.0 / GBIT, 4)
        } else if seg.ends_with("gibit") {
            (1024.0 * 1024.0 * 1024.0 / GBIT, 5)
        } else if seg.ends_with("Gbit") {
            (1024.0 * 1024.0 * 1024.0 / GBIT, 4)
        } else if seg.ends_with("tbit") {
            (1_000_000_000_000.0 / GBIT, 4)
        } else if seg.ends_with("tibit") {
            (1024.0 * 1024.0 * 1024.0 * 1024.0 / GBIT, 5)
        } else if seg.ends_with("Tbit") {
            (1024.0 * 1024.0 * 1024.0 * 1024.0 / GBIT, 4)
        } else if seg.ends_with("bit") {
            (1.0 / GBIT, 3)
        } else if seg.ends_with("kbps") {
            (1000.0 * 8.0 / GBIT, 4) // 'bps' in output actually means Bytes/sec
        } else if seg.ends_with("kibps") {
            (1024.0 * 8.0 / GBIT, 5)
        } else if seg.ends_with("Kbps") {
            (1024.0 * 8.0 / GBIT, 4)
        } else if seg.ends_with("mbps") {
            (1_000_000.0 * 8.0 / GBIT, 4)
        } else if seg.ends_with("mibps") {
            (1024.0 * 1024.0 * 8.0 / GBIT, 5)
        } else if seg.ends_with("Mbps") {
            (1024.0 * 1024.0 * 8.0 / GBIT, 4)
        } else if seg.ends_with("gbps") {
            (1_000_000_000.0 * 8.0 / GBIT, 4)
        } else if seg.ends_with("gibps") {
            (1024.0 * 1024.0 * 1024.0 * 8.0 / GBIT, 5)
        } else if seg.ends_with("Gbps") {
            (1024.0 * 1024.0 * 1024.0 * 8.0 / GBIT, 4)
        } else if seg.ends_with("tbps") {
            (1_000_000_000_000.0 * 8.0 / GBIT, 4)
        } else if seg.ends_with("tibps") {
            (1024.0 * 1024.0 * 1024.0 * 1024.0 * 8.0 / GBIT, 5)
        } else if seg.ends_with("Tbps") {
            (1024.0 * 1024.0 * 1024.0 * 1024.0 * 8.0 / GBIT, 4)
        } else if seg.ends_with("bps") {
            (8.0 / GBIT, 3)
        } else {
            (1.0 / GBIT, 0) // no unit means bit
        };
        let num = seg[..seg.len() - tail].parse::<f64>()?;
        Ok(num * multiplier)
    }

    /// Parse the output line into (delay, jitter, rate) values.
    fn parse_output_line(
        line: &str,
    ) -> Result<(f64, f64, f64), SummersetError> {
        let (mut delay, mut jitter, mut rate) = (0.0, 0.0, 0.0);
        let (mut stage, mut idx) = (0, 0);
        for seg in line.split_ascii_whitespace() {
            if seg == "netem" {
                stage = 1;
            } else if stage > 0 && seg == "delay" {
                stage = 2;
                idx = 0;
            } else if stage > 0 && seg == "rate" {
                stage = 3;
                idx = 0;
            } else if stage == 2 && idx == 0 {
                delay = Self::parse_time_ms(seg)?;
                idx += 1;
            } else if stage == 2 && idx == 1 {
                jitter = Self::parse_time_ms(seg)?;
                idx += 1;
            } else if stage == 3 && idx == 0 {
                rate = Self::parse_rate_gbps(seg)?;
                idx += 1;
            }
        }
        Ok((delay, jitter, rate))
    }

    /// Creates a new qdisc info struct.
    pub fn new() -> Self {
        QdiscInfo {
            delay: 1.0,
            jitter: 0.0,
            rate: 1.0,
        }
    }

    /// Updates my fields with a new query.
    pub fn update(&mut self) -> Result<(), SummersetError> {
        let line = Self::run_qdisc_show()?;
        let (delay, jitter, rate) = Self::parse_output_line(&line)?;
        debug_assert!(delay >= 0.0);
        debug_assert!(jitter >= 0.0);
        debug_assert!(rate >= 0.0);

        self.delay = delay;
        self.jitter = jitter;
        self.rate = rate;
        Ok(())
    }
}

#[cfg(test)]
mod qdisc_tests {
    use super::*;
    use std::process::Command;

    #[test]
    fn qdisc_run_cmd() -> Result<(), SummersetError> {
        // just testing if command running is functional here
        let output = String::from_utf8(
            Command::new("echo").arg("hello").output()?.stdout,
        )?;
        assert_eq!(output.trim(), "hello");
        Ok(())
    }
}
