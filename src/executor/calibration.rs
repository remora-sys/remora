// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::cmp::Ordering;
use std::time::Duration;
use tokio::time::Instant;

/// Utility functions for calibrating CPU-bound work to run for a specific duration
pub struct Calibration;

impl Calibration {
    /// Simulate CPU-bound work by running a computation for the specified number of iterations
    pub fn calibrated_work(iterations: u64) {
        for _ in 0..iterations {
            std::hint::spin_loop();
        }
    }

    /// Calibrate to determine how many iterations are needed to run for the target_duration
    pub fn calibrate(target_duration: Duration) -> u64 {
        let mut iterations = 1_000_000;
        let mut step_size = iterations;

        loop {
            let start = Instant::now();

            Self::calibrated_work(iterations);

            let elapsed = start.elapsed();

            match elapsed.cmp(&target_duration) {
                Ordering::Greater => {
                    // Use a binary reduction approach to converge faster
                    step_size = (step_size as f64 * 0.5) as u64;
                    if step_size == 0 {
                        break; // Stop when step size is too small to adjust further
                    }
                    iterations -= step_size;
                }
                Ordering::Less => {
                    // Increase faster initially, then adjust slowly
                    step_size = (step_size as f64 * 0.5) as u64;
                    iterations += step_size;
                }
                Ordering::Equal => break,
            }

            // If the duration is very close to target, break early
            if (elapsed.as_secs_f64() - target_duration.as_secs_f64()).abs() < 0.000_001 {
                break;
            }
        }
        iterations
    }
}
