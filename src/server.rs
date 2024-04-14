use std::{error::Error, f32::consts::E, io::{BufReader, Read, Write}, net::{SocketAddr, TcpListener, TcpStream}, result, time::Duration};

use serde::{Deserialize, Serialize};

use crate::{kv::{command::KVCommand, KVError}, KvsEngine};

use self::command::StorageEngine;


pub mod config;
pub mod command;

pub type Result<T>=result::Result<T,ServerError>;

#[derive(Debug)]
pub enum ServerError{
    BindError(&'static str),
    EngineStartUpError(&'static str),
    EngineOperationError(&'static str),
    CommandParseError(&'static str)
}

#[derive(Deserialize,Serialize)]
pub enum ServerResponse<T>{
    Success(T),
    Error(ErrorType)
}

#[derive(Deserialize,Serialize,Debug)]
pub enum ErrorType{
    OperationError,
    KeyNotFound,
}

#[derive(Serialize,Deserialize)]
pub struct StorageMetaData{
    pub engine:Option<StorageEngine>
}

pub struct Server{
    socket:TcpListener,
    engine:Box<dyn KvsEngine>
}

impl Server {   
    pub fn new(addr:impl Into<SocketAddr>,engine:impl Into<Box<dyn KvsEngine>>)->Result<Server>{
        let addr=addr.into();
        let engine=engine.into();
        eprintln!("{} {} with addr {}",engine.name(),env!("CARGO_PKG_VERSION"),addr);
        Ok(Server{
            socket:TcpListener::bind(addr).inspect_err(|e|println!("{e}")).map_err(|_|ServerError::BindError("Server::new1"))?,
            engine
        })
    }

    pub fn start(&mut self){
        for mut connection in self.socket.incoming().flatten(){
            if connection.set_read_timeout(Some(Duration::from_millis(100))).is_err(){
                continue;
            }
            
            
            match Self::parse_command(&mut connection){
                Ok(command) => {
                    
                    Self::dispatch(&mut self.engine,&mut connection,command)
                },
                Err(_) => {
                    
                    let _ = writeln!(connection,"Invalid kv command format");
                },
            }
        }
    }

    fn parse_command(connection:&mut TcpStream)->Result<KVCommand>{
        let buf=BufReader::new(connection);
        let mut stream_deserializer=serde_json::Deserializer::from_reader(buf).into_iter();
        match stream_deserializer.next(){
            Some(Ok(cmd)) => Ok(cmd),
            _ => Err(ServerError::CommandParseError("Server::parse_command1")),
        }
    }

    fn dispatch(engine:&mut Box<dyn KvsEngine>,connection:&mut TcpStream,command:KVCommand){
        match command {
            KVCommand::Get { key } => {
                
                Self::send_result(
                    connection,
                    engine.get(key).map_err(|_|ErrorType::OperationError)
                )
            },
            KVCommand::Set { key, value } => {
                
                Self::send_result(
                    connection,
                    engine.set(key, value).map_err(|_|ErrorType::OperationError)
                )
            },
            KVCommand::Rm { key } => {
                
                let res=match engine.remove(key) {
                    Ok(_) => Ok(()),
                    Err(KVError::KeyNotFound(_)) => Err(ErrorType::KeyNotFound),
                    _=> Err(ErrorType::OperationError)
                };
                Self::send_result(
                    connection,
                    res
                )
            },
        }
    }

    fn send_result<T>(connection:&mut TcpStream,res:result::Result<T,ErrorType>)
    where
        T:Serialize
    {   
        let response=match res {
            Ok(v) => ServerResponse::Success(v),
            Err(err) => ServerResponse::Error(err),
        };
        let _=serde_json::to_writer(connection, &response);
    }
}