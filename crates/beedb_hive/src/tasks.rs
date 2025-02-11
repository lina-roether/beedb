use std::{
	sync::{
		atomic::{AtomicBool, Ordering},
		Arc,
	},
	thread,
	time::{Duration, SystemTime},
};

#[derive(Clone)]
pub(crate) struct FailureStrategy {
	pub fatal: bool,
	pub retries: usize,
}

impl Default for FailureStrategy {
	fn default() -> Self {
		Self {
			fatal: false,
			retries: 3,
		}
	}
}

pub(crate) struct Timer {
	last_run: SystemTime,
	period: Duration,
	active: Arc<AtomicBool>,
}

impl Timer {
	pub fn new(period: Duration) -> (Self, TimerHandle) {
		let active = Arc::new(AtomicBool::new(true));
		let timer = Self {
			last_run: SystemTime::now(),
			period,
			active: Arc::clone(&active),
		};
		(timer, TimerHandle { active })
	}

	pub fn wait(&self) -> bool {
		if !self.active.load(Ordering::Relaxed) {
			return false;
		}
		let duration = self.period.saturating_sub(
			SystemTime::now()
				.duration_since(self.last_run)
				.unwrap_or(Duration::ZERO),
		);
		thread::sleep(duration);
		true
	}

	fn reset(&mut self) {
		self.last_run = SystemTime::now();
	}
}

pub(crate) struct TimerHandle {
	active: Arc<AtomicBool>,
}

impl TimerHandle {
	pub fn stop(self) {}
}

impl Drop for TimerHandle {
	fn drop(&mut self) {
		self.active.store(false, Ordering::Relaxed);
	}
}
