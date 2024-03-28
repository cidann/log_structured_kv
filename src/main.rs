use std::fs::OpenOptions;

use clap::Parser;
use kvs::{command::KVArgs, kv, util::{KILOBYTE, MEGABYTE}, Config, KVError};


fn main() ->kvs::Result<()> {
    let config_file=OpenOptions::new()
    .read(true)
    .open("config.json")
    .map_err(|_|KVError::IOError("main"));

    let config=config_file
    .and_then(|config_file|serde_json::from_reader(config_file).map_err(|_|KVError::IOError("main")))
    .unwrap_or(Config{
        db_dir:".".to_owned(),
        merge_size:MEGABYTE,
        file_size:4*KILOBYTE
    });

    let args=KVArgs::parse();

    let mut kv_store=kv::KvStore::open(config.db_dir)?;

    match args.operations {
        kvs::command::KVCommand::Get { key } => {
            match kv_store.get(key)?{
                Some(val) => println!("{val}"),
                None =>  println!("Key not found"),
            }
        },
        kvs::command::KVCommand::Set { key, value } => kv_store.set(key, value)?,
        kvs::command::KVCommand::Rm { key } => {
            kv_store.remove(key).inspect_err(|err|{if matches!(err,KVError::KeyNotFound(_)) {println!("Key not found")}})?;
        },
    }


    Ok(())
}
