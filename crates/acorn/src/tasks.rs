use std::{
	error::Error,
	iter,
	num::NonZero,
	sync::{
		atomic::{AtomicUsize, Ordering},
		mpsc::{channel, Receiver, Sender},
	},
	thread,
};

#[cfg(test)]
use mockall::automock;

use log::warn;

use crate::consts::DEFAULT_NUM_WORKERS;

pub(crate) struct FailureStrategy {
	pub fatal: bool,
	pub retries: usize,
}

enum Task {
	Infallible {
		function: Box<dyn FnOnce() + Send>,
	},
	Fallible {
		function: Box<dyn Fn() -> Result<(), Box<dyn Error>> + Send>,
		failure_strategy: FailureStrategy,
	},
}

impl Task {
	fn run(self) {
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

pub(crate) struct TaskRunner {
	workers: Box<[Worker]>,
	next_worker: AtomicUsize,
}

impl TaskRunner {
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

#[cfg_attr(test, automock)]
pub trait TaskRunnerApi {
	fn run<F: FnOnce() + Send + 'static>(&self, cb: F);
	fn run_fallible<F: Fn() -> Result<(), Box<dyn Error>> + Send + 'static>(
		&self,
		cb: F,
		failure_strategy: FailureStrategy,
	);
}

impl TaskRunnerApi for TaskRunner {
	fn run<F: FnOnce() + Send + 'static>(&self, cb: F) {
		self.next_worker().run(Task::Infallible {
			function: Box::new(cb),
		})
	}

	fn run_fallible<F: Fn() -> Result<(), Box<dyn Error>> + Send + 'static>(
		&self,
		cb: F,
		failure_strategy: FailureStrategy,
	) {
		self.next_worker().run(Task::Fallible {
			function: Box::new(cb),
			failure_strategy,
		})
	}
}
