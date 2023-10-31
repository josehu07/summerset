//! Linear regression helpers for performance monitoring.

use crate::utils::SummersetError;

use linreg::linear_regression_of;

/// Linear regression helper struct for maintaining time-tagged datapoints
/// and computing a linear regression model upon requested.
#[derive(Debug)]
pub struct LinearRegressor {
    /// Window of currently held datapoints.
    datapoints: Vec<(f64, f64)>,

    /// Corresponding nanosec time tags of currently held datapoints.
    timestamps: Vec<u128>,

    /// Result of last calculated regression model; upon any update to the
    /// window of datapoints, this result will be invalidated to `None`.
    model: Option<(f64, f64)>,
}

impl LinearRegressor {
    /// Creates a new linear regressor helper struct.
    pub fn new() -> Self {
        LinearRegressor {
            datapoints: vec![],
            timestamps: vec![],
            model: None,
        }
    }

    /// Injects a new datapoint into the window. It is assumed that all
    /// injections must have monotonically non-decreasing time tags.
    pub fn append_sample(&mut self, t: u128, x: f64, y: f64) {
        debug_assert!(
            self.datapoints.is_empty() || t >= *self.timestamps.last().unwrap()
        );
        self.datapoints.push((x, y));
        self.timestamps.push(t);

        if self.model.is_some() {
            self.model = None;
        }
    }

    /// Discards everything with timestamp tag before given time.
    pub fn discard_before(&mut self, t: u128) {
        debug_assert_eq!(self.timestamps.len(), self.datapoints.len());
        if !self.timestamps.is_empty() {
            let mut keep = self.timestamps.len();
            for i in 0..self.timestamps.len() {
                if self.timestamps[i] >= t {
                    keep = i;
                    break;
                }
            }

            self.timestamps.drain(0..keep);
            self.datapoints.drain(0..keep);

            if self.model.is_some() {
                self.model = None;
            }
        }
    }

    /// Returns the result of linear regression model calculated on the
    /// current window of datapoints. If the model is not valid right now,
    /// compute it.
    pub fn calc_model(&mut self) -> Result<(f64, f64), SummersetError> {
        if let Some(model) = self.model {
            Ok(model)
        } else {
            self.model = Some(linear_regression_of(&self.datapoints)?);
            Ok(self.model.unwrap())
        }
    }
}

#[cfg(test)]
mod linreg_tests {
    use super::*;

    #[test]
    fn append_discard() {
        let mut lg = LinearRegressor::new();
        assert!(lg.timestamps.is_empty());
        assert!(lg.datapoints.is_empty());
        lg.append_sample(1, 0.5, 1.1);
        lg.append_sample(3, 1.8, 2.3);
        lg.append_sample(7, 0.9, 1.8);
        assert_eq!(lg.timestamps.len(), 3);
        assert_eq!(lg.datapoints.len(), 3);
        lg.discard_before(4);
        assert_eq!(lg.timestamps.len(), 1);
        assert_eq!(lg.datapoints.len(), 1);
    }

    #[test]
    fn calc_model() -> Result<(), SummersetError> {
        let mut lg = LinearRegressor::new();
        assert!(lg.model.is_none());
        lg.append_sample(1, 1.0, 1.0);
        lg.append_sample(5, 2.0, 2.0);
        assert_eq!(lg.calc_model()?, (1.0, 0.0));
        assert_eq!(lg.calc_model()?, (1.0, 0.0));
        lg.discard_before(2);
        assert!(lg.model.is_none());
        Ok(())
    }
}
