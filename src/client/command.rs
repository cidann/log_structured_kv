use std::net::SocketAddr;

use clap::Parser;

use crate::kv::command;
use crate::kv::command::KVCommand;


#[derive(Parser)]
#[command(about,version)]
pub struct ClientArgs{
    #[arg(global=true,long,default_value="127.0.0.1:4000")]
    pub addr:SocketAddr,
    #[command(subcommand)]
    pub kv_command:KVCommand,
}