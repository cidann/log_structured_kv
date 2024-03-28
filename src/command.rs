

use clap::{Parser, Subcommand};



#[derive(Parser)]
#[command(about,version)]
pub struct KVArgs{
    #[command(subcommand)]
    pub operations:KVCommand
}

#[derive(Subcommand)]
pub enum KVCommand{
    Get{key:String},
    Set{key:String,value:String},
    Rm{key:String}
}


