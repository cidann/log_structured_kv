use std::{path::PathBuf, result};
use crate::{common::KILOBYTE, KvsEngine};
use serde::{Deserialize, Serialize};
use self::{index::Index, storage::LogStorage};

mod index;
mod storage;
mod util;
pub mod config;
pub mod command;


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

pub struct KvStore{
    storage:LogStorage,
    index:Index,
    merge_threshold:usize
}


#[derive(Deserialize,Serialize,Debug)]
enum Operation{
    Get(String),
    Remove(String),
    Set(String,String)
}

impl KvStore {
    pub fn open(path:impl Into<PathBuf>)->Result<KvStore>{
        let mut path:PathBuf=path.into();
        path.push("data");
        let storage=LogStorage::load(path)?;
        let mut index=Index::new();
        index.build_index(storage.iter_entries())?;
        
        Ok(KvStore{
            storage,
            index,
            merge_threshold:10*KILOBYTE
        })

    }
    
    //just merge every right now
    fn merge(&mut self)->Result<()>{
        let file_serial:Vec<_>=self.storage
        .iter_read_files()
        .map(|(serial,_)|serial)
        .cloned()
        .collect();

        //just collect all operation in memory right now
        //In real system needs to limit operation in memory using take or take_while
        let operations=self.index.iter().collect::<Result<Vec<_>>>()?;
        let merge_result=self.storage.merge(&file_serial,operations)?;

        assert!(self.index.len()==merge_result.len(),"Number of key before and after merge is not the same");

        
        for (log_ptr,op) in merge_result{
            if let Operation::Set(key,_)=op{
                self.index.set(key, log_ptr);
            } else {
                panic!("Merged result should only have set")
            }
        }

        Ok(())
    }
}

impl KvsEngine for KvStore {
    fn set(&mut self, key: String, value: String) -> Result<()> {
        let set_op=Operation::Set(key.clone(),value);
        let log_ptr=self.storage.write(set_op)?;
        self.index.set(key, log_ptr);
        if self.storage.storage_size()>self.merge_threshold{
            self.merge()?;
        }
        Ok(())
    }

    fn get(&mut self,key:String)->Result<Option<String>>{
        if !self.index.contains(&key){
            Ok(None)
        } else {
            Ok(Some(self.index.get(&key)?))
        }
    }

    fn remove(&mut self,key:String)->Result<()>{
        let rm_op=Operation::Remove(key.clone());

        if !self.index.contains(&key){
            Err(KVError::KeyNotFound("KvStore::remove"))
        } else {
            self.storage.write(rm_op)?;
            self.index.remove(&key)?;
            
            if self.storage.storage_size()>self.merge_threshold{
                self.merge()?;
            }
            Ok(())
        }
    }
    
    fn name(&self)->String {
        "kvs".to_string()
    }
}