use std::{net::SocketAddr, sync::Arc};

use codecrafters_redis::RESP;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Semaphore,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379").await?;

    let max_connections = Arc::new(Semaphore::new(10));
    loop {
        let (mut stream, addr) = listener.accept().await?;
        let Ok(permit) = max_connections.clone().acquire_owned().await else {
            stream
                .write_all(b"Too many active connections, try later")
                .await?;
            continue;
        };

        tokio::spawn(async move {
            if let Err(e) = process_connection(stream, addr).await {
                eprint!("Error handling connection from {addr}: {e:?}");
            }
            drop(permit);
        });
    }
}

async fn process_connection(mut stream: TcpStream, _addr: SocketAddr) -> anyhow::Result<()> {
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
        let RESP::String(command) = command_args[0] else {
            return Err(anyhow::anyhow!("command is not an string"));
        };
        let args = &command_args[1..];

        match command {
            "PING" => stream.write_all(b"+PONG\r\n").await?,
            "ECHO" => {
                let key = &args[0];
                stream.write_all(key.to_string().as_bytes()).await?;
            }
            _ => return Err(anyhow::anyhow!("Unknown command {command}")),
        }
    }
}
