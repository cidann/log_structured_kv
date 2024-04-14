

use clap::Parser;
use kvs::kv::{command,KVError,config::Config,Result,KvStore};
use kvs::KvsEngine;

fn main() ->Result<()> {
    let config=Config::open("config.json".into());
    let args=command::KVArgs::parse();

    let mut kv_store=KvStore::open(config.db_dir)?;

    match args.operations {
        command::KVCommand::Get { key } => {
            match kv_store.get(key)?{
                Some(val) => println!("{val}"),
                None =>  println!("Key not found"),
            }
        },
        command::KVCommand::Set { key, value } => kv_store.set(key, value)?,
        command::KVCommand::Rm { key } => {
            kv_store.remove(key).inspect_err(|err|{if matches!(err,KVError::KeyNotFound(_)) {println!("Key not found")}})?;
        },
    }


    Ok(())
}
