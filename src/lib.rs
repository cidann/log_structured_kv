
use serde::Deserialize;
use std::result;

pub mod command;
pub mod kv;
pub mod util;
pub use kv::KvStore;



pub type Result<T>=result::Result<T,KVError>;

#[derive(Debug,PartialEq)]
pub enum KVError{
    IOError(&'static str),
    ConfigError(&'static str),
    ReadError(&'static str),
    WriteError(&'static str),
    KeyNotFound(&'static str),
    ParseError(&'static str)
}

#[derive(Deserialize)]
pub struct Config{
    pub db_dir:String,
    pub file_size:usize,
    pub merge_size:usize,
}

