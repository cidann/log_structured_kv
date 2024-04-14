
pub mod server;
pub mod client;

pub mod kv;
pub mod sled;
mod common;


pub use kv::{KvStore,Result};

pub trait KvsEngine {
    fn name(&self)->String;
    fn set(&mut self, key: String, value: String) -> Result<()>;
    fn get(&mut self, key: String) -> Result<Option<String>>;
    fn remove(&mut self, key: String) -> Result<()>;
}