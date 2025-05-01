use serde::Serialize;
use std::future::Future;
use std::pin::Pin;
use uuid::Uuid;
use crate::task::OrcaStatesTypes;
pub trait OrcaTaskResult = Serialize + Send + Sync + 'static;


pub type TaskFuture<R> = Pin<Box<dyn Future<Output = R> + Send + Sync>>;

#[derive(Debug,Clone)]
pub enum OrcaData {
    Id(Uuid),
    Name(String),
    State(OrcaStatesTypes),
    Params(Vec<String>),
}
