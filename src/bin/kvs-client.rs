use clap::Parser;
use kvs::client::{command::ClientArgs, Client,Result,ClientError};

fn main()->Result<()>{
    let args=ClientArgs::parse();
    let client=Client::new(args.addr);
    match args.kv_command{
        kvs::kv::command::KVCommand::Get { key } => {
            let res=client.get(&key)?;
            match res{
                Some(v)=>println!("{v}"),
                None=>println!("Key not found")
            }
        },
        kvs::kv::command::KVCommand::Set { key, value } => {
            client.set(&key, &value)?
        },
        kvs::kv::command::KVCommand::Rm { key } => {
            client.remove(&key)?
        },
    }

    Ok(())
}