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

    /// Single calibration attempt to determine iterations needed for target_duration
    pub fn calibrate_once(target_duration: Duration) -> u64 {
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

    /// Calibrate with retry logic - removes outliers and ensures <5% variation
    pub fn calibrate(target_duration: Duration) -> u64 {
        const MAX_RETRIES: u32 = 5;
        const VARIATION_THRESHOLD: f64 = 0.05; // 5%
        const SAMPLE_SIZE: usize = 5;

        for retry in 0..MAX_RETRIES {
            // Collect multiple calibration samples
            let mut samples: Vec<u64> = (0..SAMPLE_SIZE)
                .map(|_| Self::calibrate_once(target_duration))
                .collect();

            // Remove outliers (min and max)
            samples.sort_unstable();
            if samples.len() > 2 {
                samples.remove(0); // Remove min
                samples.pop(); // Remove max
            }

            // Calculate variation
            if !samples.is_empty() {
                let min_val = *samples.iter().min().unwrap() as f64;
                let max_val = *samples.iter().max().unwrap() as f64;
                let avg = samples.iter().sum::<u64>() as f64 / samples.len() as f64;

                let variation = if avg > 0.0 {
                    (max_val - min_val) / avg
                } else {
                    0.0
                };

                if variation <= VARIATION_THRESHOLD {
                    return avg as u64;
                }

                // Log retry attempt for debugging
                if retry < MAX_RETRIES - 1 {
                    eprintln!(
                        "Calibration retry {}: variation {:.1}% (target: {:.1}%)",
                        retry + 1,
                        variation * 100.0,
                        VARIATION_THRESHOLD * 100.0
                    );
                }
            }
        }

        // Fallback: return single calibration attempt
        eprintln!(
            "Warning: Calibration variation still high after {} retries",
            MAX_RETRIES
        );
        Self::calibrate_once(target_duration)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_calibration_accuracy() {
        let target_duration = Duration::from_micros(800);

        // Calibrate to find the right number of iterations
        let iterations = Calibration::calibrate(target_duration);

        // Measure actual time for the calibrated iterations
        let start = Instant::now();
        Calibration::calibrated_work(iterations);
        let actual_duration = start.elapsed();

        // Check if actual duration is close to target (within 10% tolerance)
        let tolerance = 0.1;
        let diff_ratio = (actual_duration.as_secs_f64() - target_duration.as_secs_f64()).abs()
            / target_duration.as_secs_f64();

        println!(
            "Target: {:?}, Actual: {:?}, Iterations: {}, Diff: {:.2}%",
            target_duration,
            actual_duration,
            iterations,
            diff_ratio * 100.0
        );

        assert!(
            diff_ratio < tolerance,
            "Calibration inaccurate: expected ~{:?}, got {:?} (difference: {:.2}%)",
            target_duration,
            actual_duration,
            diff_ratio * 100.0
        );
    }
}
