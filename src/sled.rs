use sled::{Db, Tree};

use crate::{kv::KVError, KvsEngine};


impl KvsEngine for Db {
    fn set(&mut self, key: String, value: String) -> crate::Result<()> {
        self.insert(key, value.as_bytes()).map_err(|_|KVError::WriteError("Sled::set1"))?;
        self.flush().map_err(|_|KVError::WriteError("Sled::set2"))?;

        Ok(())
    }

    fn get(&mut self, key: String) -> crate::Result<Option<String>> {
        let ivec=Tree::get(self, key).map_err(|_|KVError::ReadError("Sled::get1"))?;
        ivec.map_or(
            Ok(None),
            |ivec|
            String::from_utf8(ivec.to_vec())
            .map_or(Err(KVError::ReadError("Sled::get2")),|s|Ok(Some(s)))
        )
    }

    fn remove(&mut self, key: String) -> crate::Result<()> {
        Tree::remove(self, key)
        .map_err(|_|KVError::WriteError("Sled::remove1"))?
        .ok_or(KVError::KeyNotFound("Sled::remove2"))?;
        self.flush().map_err(|_|KVError::WriteError("Sled::remove3"))?;

        Ok(())
    }
    
    fn name(&self)->String {
        "sled".to_string()
    }
}