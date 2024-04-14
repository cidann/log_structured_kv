use std::{io::BufReader, net::{SocketAddr, TcpStream}, result};

use crate::{kv::command::KVCommand, server};


pub mod config;
pub mod command;

pub type Result<T>=result::Result<T,ClientError>;

#[derive(Debug)]
pub enum ClientError{
    ConnectionError(&'static str),
    OperationError(&'static str),
    KeyNotFound(&'static str)
}

pub struct Client{
    addr:SocketAddr
}

impl Client {
    pub fn new(addr:SocketAddr)->Client{
        
        Client{
            addr
        }
    }

    pub fn get(&self,key:&str)->Result<Option<String>>{
        let sock=TcpStream::connect(self.addr).map_err(|_|ClientError::ConnectionError("Client::get1"))?;
        let cmd=KVCommand::Get { key:key.to_string()};
        serde_json::to_writer(&sock, &cmd)
        .map_err(|_|ClientError::OperationError("Client::get1"))?;
        

        let buf=BufReader::new(&sock);
        let response:result::Result<server::ServerResponse<Option<String>>,_>=serde_json::from_reader(buf);
        match response {
            Ok(server::ServerResponse::Success(v)) => Ok(v),
            _ => Err(ClientError::OperationError("Client::get2")),
        }
    }

    pub fn set(&self,key:&str,value:&str)->Result<()>{
        let sock=TcpStream::connect(self.addr).map_err(|_|ClientError::ConnectionError("Client::set1"))?;
        let cmd=KVCommand::Set { key: key.to_string(), value: value.to_string() };
        serde_json::to_writer(&sock, &cmd)
        .map_err(|_|ClientError::OperationError("Client::set1"))?;
        

        let buf=BufReader::new(&sock);
        let response:result::Result<server::ServerResponse<()>,_>=serde_json::from_reader(buf);
        match response {
            Ok(server::ServerResponse::Success(_)) => Ok(()),
            _ => Err(ClientError::OperationError("Client::set2")),
        }
    }

    pub fn remove(&self,key:&str)->Result<()>{
        let sock=TcpStream::connect(self.addr).map_err(|_|ClientError::ConnectionError("Client::remove1"))?;
        let cmd=KVCommand::Rm{ key: key.to_string()};
        serde_json::to_writer(&sock, &cmd)
        .map_err(|_|ClientError::OperationError("Client::remove1"))?;
        

        let buf=BufReader::new(&sock);
        let response:result::Result<server::ServerResponse<()>,_>=serde_json::from_reader(buf);
        match response {
            Ok(server::ServerResponse::Success(_)) => Ok(()),
            Ok(server::ServerResponse::Error(server::ErrorType::KeyNotFound))=>Err(ClientError::KeyNotFound("Key not found")),
            _ => Err(ClientError::OperationError("Client::remove2")),
        }
    }
}