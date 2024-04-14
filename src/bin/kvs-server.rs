use std::{fs::{DirBuilder, OpenOptions}, io::BufReader, os::unix::fs::DirBuilderExt, path::PathBuf};

use clap::Parser;
use kvs::{kv::config::Config, server::{command::{ServerArgs, StorageEngine}, Result, Server, ServerError, StorageMetaData}, KvStore, KvsEngine};

fn main()->Result<()>{
    let args=ServerArgs::parse();
    //need to verify ip later
    let engine=args.engine.unwrap_or(StorageEngine::Kv);
    let config=Config::open("config.json".into());

    let data_path:PathBuf=PathBuf::from(config.db_dir).join("data");
    let db_path=data_path.join("db");
    let meta_path=data_path.join("metadata");
    DirBuilder::new()
    .recursive(true)
    .create(data_path)
    .map_err(|_|ServerError::EngineOperationError("failed to create/open data dir"))?;

    let meta_file=OpenOptions::new()
    .create(true)
    .read(true)
    .write(true)
    .open(meta_path)
    .map_err(|_|ServerError::EngineOperationError("failed to create/open metadata file"))?;

    let buf=BufReader::new(&meta_file);
    let storage_meta:StorageMetaData=match serde_json::from_reader(buf){
        Ok(v) => v,
        Err(e) => {
            if e.is_data()||e.is_syntax(){
                return Err(ServerError::EngineOperationError("failed to parse metadata file"));
            }
            StorageMetaData{
                engine:None
            }
        },
    };

    let engine:Box<dyn KvsEngine>=match (engine,&storage_meta.engine) {
        (StorageEngine::Kv,None|Some(StorageEngine::Kv)) => {
            if storage_meta.engine.is_none(){
                serde_json::to_writer(&meta_file, &StorageMetaData{engine:Some(StorageEngine::Kv)})
                .map_err(|_|ServerError::EngineOperationError("failed to write metadata"))?
            }
            let kv=KvStore::open(db_path)
            .map_err(|_|ServerError::EngineStartUpError("kvs"))?;
            Box::new(kv)
        },
        (StorageEngine::Sled,None|Some(StorageEngine::Sled)) => {
            if storage_meta.engine.is_none(){
                serde_json::to_writer(&meta_file, &StorageMetaData{engine:Some(StorageEngine::Sled)})
                .map_err(|_|ServerError::EngineOperationError("failed to write metadata"))?
            }
            let sled=sled::open(db_path)
            .map_err(|_|ServerError::EngineStartUpError("sled"))?;
            Box::new(sled)
        },
        _=> return Err(ServerError::EngineOperationError("invalid engine"))
    };
    let mut server=Server::new(args.addr, engine)?;
    server.start();

    Ok(())
}