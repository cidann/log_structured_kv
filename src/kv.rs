use std::path::PathBuf;
use crate::{ util::KILOBYTE, KVError, Result};



use serde::{Deserialize, Serialize};



use self::{index::Index, storage::LogStorage};

mod index;
mod storage;


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
        let path:PathBuf=path.into();
        let storage=LogStorage::load(path)?;
        let mut index=Index::new();
        index.build_index(storage.iter_entries())?;
        

        Ok(KvStore{
            storage,
            index,
            merge_threshold:10*KILOBYTE
        })


    }


    pub fn set(&mut self,key:String,val:String)->Result<()>{
        let set_op=Operation::Set(key.clone(),val);
        let log_ptr=self.storage.write(set_op)?;
        self.index.set(key, log_ptr);
        if self.storage.storage_size()>self.merge_threshold{
            self.merge()?;
        }
        Ok(())
    }

    pub fn get(&self,key:String)->Result<Option<String>>{
        if !self.index.contains(&key){
            Ok(None)
        } else {
            Ok(Some(self.index.get(&key)?))
        }
    }

    pub fn remove(&mut self,key:String)->Result<()>{
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