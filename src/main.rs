use std::{collections::HashMap, sync::Arc};

use codecrafters_redis::RESP;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Mutex,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379").await?;

    // basic shared cache
    let data: Arc<Mutex<HashMap<String, RESP>>> = Arc::new(Mutex::new(HashMap::new()));

    loop {
        let (stream, _addr) = listener.accept().await?;
        // Clone/Increase ref count out of spawn so it can be moved
        let data = data.clone();
        tokio::spawn(async move {
            if let Err(e) = process_connection(stream, data).await {
                eprintln!("{e:?}");
            }
        });
    }
}

async fn process_connection(
    mut stream: TcpStream,
    cache: Arc<Mutex<HashMap<String, RESP>>>,
) -> anyhow::Result<()> {
    let mut buf = [0u8; 1024];
    loop {
        let bytes_read = stream.read(&mut buf).await?;
        let data = RESP::from_str(str::from_utf8(&buf[..bytes_read])?)?;
        eprintln!("{data:?}");
        // data should be an array, commands
        let RESP::Array(command_args) = data else {
            return Err(anyhow::anyhow!(
                "expected an array with command and arguments, got {data:?}"
            ));
        };
        let command = match &command_args[0] {
            RESP::SimpleString(command) => command,
            RESP::Bulk(command) => &String::from_utf8(command.clone())?,
            _ => {
                return Err(anyhow::anyhow!("Command is not a string"));
            }
        };
        let args = &command_args[1..];

        match command.as_str() {
            "PING" => stream.write_all(b"+PONG\r\n").await?,
            "ECHO" => {
                let key = &args[0];
                stream.write_all(key.to_string().as_bytes()).await?;
            }
            "SET" => {
                // some maybe incorrect assumptions
                // there's two more values
                // 1: A String
                // 2: Some RESP
                let key = match &args[0] {
                    RESP::SimpleString(key) => key,
                    RESP::Bulk(key) => &String::from_utf8(key.clone())?,
                    _ => {
                        return Err(anyhow::anyhow!("Key is not a string"));
                    }
                };
                let mut locked_cache = cache.lock().await;
                locked_cache.insert(key.clone(), args[1].clone());
                stream.write_all(b"+OK\r\n").await?;
            }
            "GET" => {
                // some maybe incorrect assumptions
                // there's one more values
                // 1: A String
                let key = match &args[0] {
                    RESP::SimpleString(key) => key,
                    RESP::Bulk(key) => &String::from_utf8(key.clone())?,
                    _ => {
                        return Err(anyhow::anyhow!("Key is not a string"));
                    }
                };
                let locked_cache = cache.lock().await;
                let val = locked_cache.get(key);
                let to_write = match val {
                    Some(v) => &v.to_string(),
                    None => &RESP::null_bulk().to_string(),
                };
                stream.write_all(to_write.as_bytes()).await?;
            }
            _ => return Err(anyhow::anyhow!("Unknown command {command}")),
        }
    }
}
