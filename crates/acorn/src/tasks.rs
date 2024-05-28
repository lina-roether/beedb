use std::{
	error::Error,
	io,
	pin::Pin,
	sync::{
		atomic::{AtomicBool, Ordering},
		Arc,
	},
	thread,
	time::{Duration, SystemTime},
};

use futures::{executor::ThreadPool, Future};
use log::warn;
#[cfg(test)]
use mockall::automock;

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
	pub fn new(period: Duration) -> (Self, Arc<AtomicBool>) {
		let active = Arc::new(AtomicBool::new(true));
		let timer = Self {
			last_run: SystemTime::now(),
			period,
			active: Arc::clone(&active),
		};
		(timer, active)
	}

	fn sleep_duration(&self) -> Option<Duration> {
		if !self.active.load(Ordering::Relaxed) {
			return None;
		}
		Some(
			self.period.saturating_sub(
				SystemTime::now()
					.duration_since(self.last_run)
					.unwrap_or(Duration::ZERO),
			),
		)
	}

	fn reset(&mut self) {
		self.last_run = SystemTime::now();
	}
}

pub(crate) struct ScheduledTaskHandle {
	running: Arc<AtomicBool>,
}

impl Drop for ScheduledTaskHandle {
	fn drop(&mut self) {
		self.running.store(false, Ordering::Relaxed);
	}
}

pub(crate) trait TaskFuture = Future<Output = ()> + Send;
pub(crate) trait FallibleTaskFuture = Future<Output = Result<(), Box<dyn Error>>> + Send;
pub(crate) trait Task = (Fn() -> Pin<Box<dyn TaskFuture>>) + Send;

pub trait IntoTask {
	fn into_task(self) -> Box<dyn Task>;
}

impl<F: TaskFuture + Send + 'static, T: (Fn() -> F) + Send + 'static> IntoTask for T {
	fn into_task(self) -> Box<dyn Task> {
		Box::new(move || Box::pin((self)()))
	}
}

pub(crate) struct FallibleTask<F: FallibleTaskFuture, B: Fn() -> F> {
	future_builder: B,
	failure_strategy: FailureStrategy,
}

impl<F: FallibleTaskFuture, B: Fn() -> F> FallibleTask<F, B> {
	fn new(future_builder: B, failure_strategy: FailureStrategy) -> Self {
		Self {
			future_builder,
			failure_strategy,
		}
	}
}

impl<F: FallibleTaskFuture + Send, B: (Fn() -> F) + Send + Sync + 'static> IntoTask
	for FallibleTask<F, B>
{
	fn into_task(self) -> Box<dyn Task> {
		let future_builder = Arc::new(self.future_builder);
		Box::new(move || {
			let builder = Arc::clone(&future_builder);
			Box::pin(async move {
				let mut num_tries = self.failure_strategy.retries;
				let err = loop {
					let Err(err) = (builder)().await else {
						return;
					};
					if num_tries == 0 {
						break err;
					} else {
						warn!("An asynchronous task failed ({num_tries} retries remaining): {err}");
						num_tries -= 1;
						continue;
					}
				};
				if self.failure_strategy.fatal {
					panic!("A required asynchronous task failed: {err}");
				} else {
					warn!("An asynchronous task failed: {err}");
				}
			})
		})
	}
}

pub(crate) struct TaskRunner {
	pool: ThreadPool,
}

impl TaskRunner {
	pub fn new() -> Result<Self, io::Error> {
		Ok(Self {
			pool: ThreadPool::new()?,
		})
	}
}

#[cfg_attr(test, automock(
    type ScheduledTaskHandle = ();
))]
pub(crate) trait TaskRunnerApi {
	type ScheduledTaskHandle;

	fn run<T: IntoTask + 'static>(&self, task: T);
	fn schedule<F: IntoTask + 'static>(
		&self,
		task: F,
		period: Duration,
	) -> Self::ScheduledTaskHandle;
}

impl TaskRunnerApi for TaskRunner {
	type ScheduledTaskHandle = ScheduledTaskHandle;

	fn run<T: IntoTask + 'static>(&self, task: T) {
		self.pool.spawn_ok(task.into_task()());
	}

	fn schedule<F: IntoTask + 'static>(
		&self,
		task: F,
		period: Duration,
	) -> Self::ScheduledTaskHandle {
		let task = task.into_task();
		let (timer, running) = Timer::new(period);
		self.pool.spawn_ok(async move {
			while let Some(duration) = timer.sleep_duration() {
				thread::sleep(duration);
				task().await
			}
		});
		ScheduledTaskHandle { running }
	}
}
