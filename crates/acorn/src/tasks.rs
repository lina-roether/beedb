use std::{
	error::Error,
	io, iter, mem,
	num::NonZero,
	sync::{
		atomic::{AtomicBool, AtomicUsize, Ordering},
		mpsc::{channel, Receiver, Sender},
		Arc,
	},
	thread,
	time::{Duration, SystemTime},
};

use futures::{executor::ThreadPool, Future};
#[cfg(test)]
use mockall::{automock, concretize};

use log::warn;
use parking_lot::Mutex;

use crate::consts::DEFAULT_NUM_WORKERS;

#[derive(Clone)]
pub(crate) struct FailureStrategy {
	pub fatal: bool,
	pub retries: usize,
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

trait TaskFuture = Future<Output = ()>;
trait FallibleTaskFuture = Future<Output = Result<(), Box<dyn Error>>>;

pub(crate) struct TaskRunner {
	pool: ThreadPool,
}

impl TaskRunner {
	pub fn new() -> Result<Self, io::Error> {
		Self {
			pool: ThreadPool::new(),
		}
	}
}

#[cfg_attr(test, automock(
    type ScheduledTaskHandle = MockScheduledTaskHandleApi;
))]
pub(crate) trait TaskRunnerApi {
	type ScheduledTaskHandle: ScheduledTaskHandleApi;

	#[cfg_attr(test, concretize)]
	fn run<F: TaskFn + 'static>(&self, cb: F);
	#[cfg_attr(test, concretize)]
	fn run_fallible<F: FallibleTaskFn + 'static>(&self, cb: F, failure_strategy: FailureStrategy);
	#[cfg_attr(test, concretize)]
	fn schedule<F: TaskFn + 'static>(&self, cb: F, period: Duration) -> Self::ScheduledTaskHandle;
	#[cfg_attr(test, concretize)]
	fn schedule_fallible<F: FallibleTaskFn + 'static>(
		&self,
		cb: F,
		period: Duration,
		failure_strategy: FailureStrategy,
	) -> Self::ScheduledTaskHandle;
}

impl TaskRunnerApi for TaskRunner {
	type ScheduledTaskHandle = ScheduledTaskHandle;

	fn run<F: Fn() + Send + Sync + 'static>(&self, cb: F) {
		self.pool.next_worker().run(Task::new_infallible(cb))
	}

	fn run_fallible<F: Fn() -> Result<(), Box<dyn Error>> + Send + Sync + 'static>(
		&self,
		cb: F,
		failure_strategy: FailureStrategy,
	) {
		self.pool
			.next_worker()
			.run(Task::new_fallible(cb, failure_strategy))
	}

	fn schedule<F: TaskFn + 'static>(&self, cb: F, period: Duration) -> ScheduledTaskHandle {
		self.scheduler
			.schedule_task(Task::new_infallible(cb), period)
	}

	fn schedule_fallible<F: FallibleTaskFn + 'static>(
		&self,
		cb: F,
		period: Duration,
		failure_strategy: FailureStrategy,
	) -> ScheduledTaskHandle {
		self.scheduler
			.schedule_task(Task::new_fallible(cb, failure_strategy), period)
	}
}
