use serde_json::StreamDeserializer;

use crate::{KVError, Result};

pub const KILOBYTE:usize=1000;
pub const MEGABYTE:usize=KILOBYTE*1000;
pub const GIGABYTE:usize=MEGABYTE*1000;


pub struct OffsetStreamSerializer<'de,R,T>{
    stream:StreamDeserializer<'de,R,T>
}

pub struct OffsetVec<T>{
    vec:Vec<T>,
    offset:usize
}


impl<'de,R,T>  OffsetStreamSerializer<'de,R,T> {
    pub fn new(stream:StreamDeserializer<'de,R,T>)->OffsetStreamSerializer<'de,R,T>{
        OffsetStreamSerializer{
            stream
        }
    }
}

impl<'de,R,T> Iterator for OffsetStreamSerializer<'de,R,T> 
    where
    R: serde_json::de::Read<'de>,
    T: serde::de::Deserialize<'de>,
{
    type Item=Result<(u64,T)>;

    fn next(&mut self) -> Option<Self::Item> {
        let offset=self.stream.byte_offset() as u64;
        let val=self.stream.next()?;
        match val {
            Ok(val) => Some(Ok((offset,val))),
            Err(_) => Some(Err(KVError::ParseError("OffsetStreamSerializer::next"))),
        }
    }
}

impl<T> OffsetVec<T>{
    pub fn new(offset:usize)->OffsetVec<T>{
        OffsetVec{
            vec:Vec::new(),
            offset
        }
    }

    pub fn get(&self,index:usize)->&T{
        &self.vec[index-self.offset]
    }

    pub fn set(&mut self,index:usize,data:T){
        self.vec[index-self.offset]=data
    }

    pub fn push(&mut self,data:T)->usize{
        self.vec.push(data);
        self.vec.len()+self.offset-1
    }

    pub fn pop(&mut self)->Option<T>{
        self.vec.pop()
    }

    pub fn next_index(&self)->usize{
        self.vec.len()+self.offset
    }

    pub fn first(&self)->Option<(usize,&T)>{
        self.vec.first().map(|first|(self.offset,first))
    }

    pub fn last(&self)->Option<(usize,&T)>{
        self.vec.last().map(|first|(self.offset+self.vec.len()-1,first))
    }

    pub fn iter(&self)->impl Iterator<Item = &T>{
        self.vec.iter()
    }

    pub fn iter_offset(&self)->impl Iterator<Item = (usize,&T)>{
        self.vec.iter().enumerate().map(|(enu,data)|(enu+self.offset,data))
    }
}
