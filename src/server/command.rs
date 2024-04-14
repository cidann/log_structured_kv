use std::{fmt::Debug, net::SocketAddr, str::FromStr};

use clap::{Parser, ValueEnum};
use serde::{Deserialize, Serialize};
use serde_json::from_str;
use std::error::Error;

use crate::client::command;

#[derive(Parser)]
#[command(about,version)]
pub struct ServerArgs{
    #[arg(long,default_value="127.0.0.1:4000")]
    pub addr:SocketAddr,
    #[arg(long,value_enum)]
    pub engine:Option<StorageEngine>
}


#[derive(ValueEnum,Clone,Deserialize,Serialize)]
pub enum StorageEngine{
    #[clap(name = "kvs")]
    Kv,
    Sled
}
