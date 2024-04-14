use clap::{Parser, Subcommand};
use serde::{Deserialize, Serialize};



#[derive(Parser)]
#[command(about,version)]
pub struct KVArgs{
    #[command(subcommand)]
    pub operations:KVCommand
}

#[derive(Subcommand,Deserialize,Serialize)]
pub enum KVCommand{
    Get{key:String},
    Set{key:String,value:String},
    Rm{key:String}
}


