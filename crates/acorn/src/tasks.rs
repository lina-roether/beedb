use std::{
	error::Error,
	iter, mem,
	num::NonZero,
	sync::{
		atomic::{AtomicBool, AtomicUsize, Ordering},
		mpsc::{channel, Receiver, Sender},
		Arc,
	},
	thread,
	time::{Duration, SystemTime},
};

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

pub(crate) trait TaskFn = Fn() + Send + Sync;
pub(crate) trait FallibleTaskFn = Fn() -> Result<(), Box<dyn Error>> + Send + Sync;

#[derive(Clone)]
enum Task {
	Infallible {
		function: Arc<Box<dyn TaskFn>>,
	},
	Fallible {
		function: Arc<Box<dyn FallibleTaskFn>>,
		failure_strategy: FailureStrategy,
	},
}

impl Task {
	fn new_infallible<F: TaskFn + 'static>(cb: F) -> Self {
		Self::Infallible {
			function: Arc::new(Box::new(cb)),
		}
	}

	fn new_fallible<F: FallibleTaskFn + 'static>(cb: F, failure_strategy: FailureStrategy) -> Self {
		Self::Fallible {
			function: Arc::new(Box::new(cb)),
			failure_strategy,
		}
	}

	fn run(&self) {
		match self {
			Self::Infallible { function } => function(),
			Self::Fallible {
				function,
				failure_strategy,
			} => {
				let mut num_retries = failure_strategy.retries;
				let err = loop {
					if let Err(err) = function() {
						if num_retries > 0 {
							num_retries -= 1;
							continue;
						}
						break err;
					}
					return;
				};
				if failure_strategy.fatal {
					panic!("A mandatory async task failed: {err}");
				} else {
					warn!("An async task failed: {err}")
				}
			}
		}
	}
}

struct ScheduledTask {
	task: Task,
	timer: Timer,
}

enum WorkerCmd {
	Run(Task),
	Kill,
}

struct Worker {
	cmd_sender: Sender<WorkerCmd>,
}

impl Worker {
	fn new() -> Self {
		let (cmd_sender, cmd_receiver) = channel();
		thread::spawn(move || Self::worker_main(cmd_receiver));
		Self { cmd_sender }
	}

	fn run(&self, task: Task) {
		self.cmd_sender
			.send(WorkerCmd::Run(task))
			.expect("A worker was unexpectedly killed!");
	}

	fn worker_main(cmd_receiver: Receiver<WorkerCmd>) {
		for cmd in cmd_receiver {
			match cmd {
				WorkerCmd::Run(task) => task.run(),
				WorkerCmd::Kill => return,
			}
		}
	}
}

impl Drop for Worker {
	fn drop(&mut self) {
		let _ = self.cmd_sender.send(WorkerCmd::Kill);
	}
}

struct TaskScheduler {
	tasks: Arc<Mutex<Vec<ScheduledTask>>>,
	running: Arc<AtomicBool>,
}

const SCHEDULER_EMPTY_SLEEP: Duration = Duration::from_secs(1);

impl TaskScheduler {
	fn new(pool: Arc<TaskWorkerPool>) -> Self {
		let running = Arc::new(AtomicBool::new(true));
		let running_2 = Arc::clone(&running);
		let tasks = Arc::new(Mutex::new(Vec::new()));
		let tasks_2 = Arc::clone(&tasks);
		thread::spawn(move || Self::scheduler_main(running_2, tasks_2, pool));
		Self { tasks, running }
	}

	fn schedule_task(&self, task: Task, period: Duration) -> ScheduledTaskHandle {
		let (timer, running) = Timer::new(period);
		self.tasks.lock().push(ScheduledTask { task, timer });
		ScheduledTaskHandle { running }
	}

	fn scheduler_main(
		running: Arc<AtomicBool>,
		tasks: Arc<Mutex<Vec<ScheduledTask>>>,
		pool: Arc<TaskWorkerPool>,
	) {
		while running.load(Ordering::Relaxed) {
			let mut tq = tasks.lock();
			let Some((task_index, duration)) = tq
				.iter()
				.enumerate()
				.map(|(i, task)| (i, task.timer.sleep_duration()))
				.min_by_key(|(_, duration)| *duration)
			else {
				thread::sleep(SCHEDULER_EMPTY_SLEEP);
				continue;
			};
			if let Some(duration) = duration {
				mem::drop(tq);
				thread::sleep(duration);
			} else {
				tq.remove(task_index);
				continue;
			}

			let mut tq = tasks.lock();
			let task = &mut tq[task_index];
			pool.next_worker().run(task.task.clone());
			task.timer.reset();
		}
	}
}

impl Drop for TaskScheduler {
	fn drop(&mut self) {
		self.running.store(false, Ordering::Relaxed);
	}
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub(crate) struct TaskRunnerConfig {
	pub num_workers: NonZero<usize>,
}

impl Default for TaskRunnerConfig {
	fn default() -> Self {
		TaskRunnerConfig {
			num_workers: NonZero::new(DEFAULT_NUM_WORKERS).unwrap(),
		}
	}
}

struct TaskWorkerPool {
	workers: Box<[Worker]>,
	next_worker: AtomicUsize,
}

impl TaskWorkerPool {
	fn new(config: &TaskRunnerConfig) -> Self {
		Self {
			workers: iter::repeat_with(Worker::new)
				.take(config.num_workers.get())
				.collect(),
			next_worker: AtomicUsize::new(0),
		}
	}

	fn next_worker(&self) -> &Worker {
		let next_worker = self.next_worker.load(Ordering::Acquire);
		let worker = &self.workers[next_worker];
		self.next_worker
			.store((next_worker + 1) % self.workers.len(), Ordering::Release);
		worker
	}
}

pub(crate) struct ScheduledTaskHandle {
	running: Arc<AtomicBool>,
}

#[cfg_attr(test, automock)]
pub(crate) trait ScheduledTaskHandleApi {
	fn stop(self);
}

impl ScheduledTaskHandleApi for ScheduledTaskHandle {
	fn stop(self) {
		mem::drop(self)
	}
}

impl Drop for ScheduledTaskHandle {
	fn drop(&mut self) {
		self.running.store(false, Ordering::Relaxed);
	}
}

pub(crate) struct TaskRunner {
	pool: Arc<TaskWorkerPool>,
	scheduler: TaskScheduler,
}

impl TaskRunner {
	pub fn new(config: &TaskRunnerConfig) -> Self {
		let pool = Arc::new(TaskWorkerPool::new(config));
		let scheduler = TaskScheduler::new(Arc::clone(&pool));
		Self { pool, scheduler }
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
	fn schedule<F: TaskFn + 'static>(&self, cb: F, period: Duration) -> ScheduledTaskHandle;
	#[cfg_attr(test, concretize)]
	fn schedule_fallible<F: FallibleTaskFn + 'static>(
		&self,
		cb: F,
		period: Duration,
		failure_strategy: FailureStrategy,
	) -> ScheduledTaskHandle;
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
