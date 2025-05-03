use serde::Serialize;
use std::future::Future;
use std::pin::Pin;
use uuid::Uuid;
use crate::task::RunState;    

pub trait TaskResult = Serialize + Send + Sync + 'static;

pub type TaskFuture<R> = Pin<Box<dyn Future<Output = R> + Send + Sync>>;

#[derive(Debug,Clone)]
pub enum TaskData {
    Id(Uuid),
    Name(String),
    State(RunState),
}
