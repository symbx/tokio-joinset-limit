use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use tokio::task::{JoinError, JoinSet};

pub struct JoinSetLimit<O: Send + 'static> {
    set: JoinSet<O>,
    queue: VecDeque<Pin<Box<dyn Future<Output = O> + Send + 'static>>>,
    limit: usize,
}

impl<O: Send + 'static> JoinSetLimit<O> {
    /// Create a new JoinSetLimit
    /// ### Arguments
    /// * `limit` - The maximum number of tasks that can be running at once
    pub fn new(limit: usize) -> Self {
        Self {
            set: JoinSet::new(),
            queue: VecDeque::with_capacity(limit),
            limit,
        }
    }

    /// Add a task to the JoinSetLimit
    /// Spawn it if there are less than the limit, otherwise add it to the queue
    /// ### Arguments
    /// * `task` - The task to add
    pub fn spawn<R: Future<Output = O> + Send + 'static>(&mut self, task: R) {
        if self.set.len() < self.limit {
            self.set.spawn(task);
        } else {
            self.queue.push_back(Box::pin(task));
        }
    }

    /// Get the next task from the JoinSetLimit
    pub async fn next(&mut self) -> Option<Result<O, JoinError>> {
        let value = self.set.join_next().await;
        while self.set.len() < self.limit && !self.queue.is_empty() {
            if let Some(r) = self.queue.pop_front() {
                self.set.spawn(r);
            }
        }
        value
    }
}

#[cfg(test)]
mod tests {
    use crate::JoinSetLimit;
    use std::time::Duration;

    async fn example_sleep(s: u64, id: u32) -> u32 {
        println!("[{}] Start sleep for {}s", id, s);
        tokio::time::sleep(Duration::from_secs(s)).await;
        println!("[{}] End sleep for {}s", id, s);
        id
    }

    async fn another_example_sleep(s: u64, id: u32) -> u32 {
        println!("[{}] Start sleep for {}s", id, s);
        tokio::time::sleep(Duration::from_secs(s)).await;
        println!("[{}] End sleep for {}s", id, s);
        id
    }

    #[tokio::test]
    async fn test_join() {
        let mut join = JoinSetLimit::new(2);
        join.spawn(example_sleep(4, 1));
        join.spawn(example_sleep(2, 2));
        join.spawn(another_example_sleep(6, 3));
        let now = tokio::time::Instant::now();
        while let Some(v) = join.next().await {
            println!("next ({:?})", v.expect("failed to join task"));
        }
        // Very weak test.
        // Total time spend should be 2 + 6 = 8s
        // Added threshold of 100ms just in case.
        assert!(now.elapsed().as_millis().abs_diff(8000) < 100);
    }
}
