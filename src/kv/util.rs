use serde_json::StreamDeserializer;

use super::{KVError, Result};

pub struct OffsetStreamSerializer<'de,R,T>{
    stream:StreamDeserializer<'de,R,T>
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
