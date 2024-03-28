use std::collections::HashMap;





use crate::{KVError, Result};

use super::{storage::LogPointer, Operation};



pub struct Index{
    index:HashMap<String,LogPointer>,
}




impl Index {
    pub fn new()->Index{
        Index{
            index:HashMap::new()
        }
    }

    pub fn build_index(&mut self,log_iter:impl Iterator<Item = Result<(LogPointer,Operation)>>)->Result<()>{
        for parse_result in log_iter{
            let (log_ptr,operation)=parse_result?;
            
            match operation {
                Operation::Get(_) => panic!("Get operation should never be on file"),
                Operation::Remove(key) => {
                    self.index.remove(&key).expect("key does not exist during index building");
                },
                Operation::Set(key,_) => {
                    self.index.insert(key, log_ptr);
                },
            }
        }
        Ok(())
    }

    pub fn get(&self,key:&String)->Result<String>{
        let pointer=self.index.get(key).ok_or(KVError::KeyNotFound("index::get"))?;
        let operation:Operation=pointer.read()?;
        match operation {
            Operation::Set(_, val) => Ok(val),
            _=>panic!("Log pointer should only point to set operations")
        }
    }

    pub fn set(&mut self,key:String,log_ptr:LogPointer){
        self.index.insert(key, log_ptr);
    }

    pub fn remove(&mut self,key:&String)->Result<()>{
        self.index.remove(key).ok_or(KVError::KeyNotFound("index::remove"))?;
        Ok(())
    }

    pub fn contains(&self,key:&String)->bool{
        self.index.contains_key(key)
    }

    pub fn iter(&self)->impl Iterator<Item = Result<Operation>>+'_{
        self.index
        .iter()
        .map(|(_,log_ptr)|log_ptr.read())
    }

    pub fn len(&self)->usize{
        return self.index.len();
    }
}