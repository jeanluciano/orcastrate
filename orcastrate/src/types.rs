use serde::Serialize;
use std::future::Future;
use std::pin::Pin;
use uuid::Uuid;
use crate::task::RunState;    
use crate::task::OrcaError;

pub type TaskArgs = Vec<String>;

pub trait TaskResult = Serialize + Send + Sync + 'static;

pub type TaskFutureGen<R> = Pin<Box<dyn Future<Output = R> + Send + Sync>>;



pub type FutureCreatorFn<R> = Box<dyn Fn(String) -> Result<TaskFutureGen<R>, OrcaError> + Send + Sync>;


#[derive(Debug,Clone)]
pub enum TaskData {
    Id(Uuid),
    Name(String),
    State(RunState),
}
