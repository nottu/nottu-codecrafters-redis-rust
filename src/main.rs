use std::time::Duration;

use clap::Parser;
use codecrafters_redis::RESP;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::Instant,
};

use crate::{
    commands::{RedisCli, SetArgs},
    db::Db,
};

mod commands;
mod db;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379").await?;

    let cache = Db::new();

    loop {
        let (stream, _addr) = listener.accept().await?;
        // Clone/Increase ref count out of spawn so it can be moved
        let data = cache.clone();
        tokio::spawn(async move {
            if let Err(e) = process_connection(stream, data).await {
                eprintln!("{e:?}");
            }
        });
    }
}

async fn process_connection(mut stream: TcpStream, cache: Db) -> anyhow::Result<()> {
    let mut buf = [0u8; 1024];
    loop {
        let bytes_read = stream.read(&mut buf).await?;
        if bytes_read == 0 {
            eprintln!("No data read, closing stream.");
            return Ok(());
        }
        let data = str::from_utf8(&buf[..bytes_read])?;
        dbg!(data);
        let data = RESP::try_parse(data)?;
        eprintln!("Parsed: {data:?}");
        // data should be an array of strings (possibly all bulk)
        let RESP::Array(command_args) = data else {
            return Err(anyhow::anyhow!(
                "expected an array with command and arguments, got {data:?}"
            ));
        };
        let command = RedisCli::try_parse_from(
            // clap wants the first arg to be the program name... we pre-pend a value to comply
            std::iter::once(RESP::SimpleString("redis-cli".to_string())).chain(command_args),
        )?;
        match command.command {
            commands::Command::Ping => {
                stream.write_all(b"+PONG\r\n").await?;
            }
            commands::Command::Echo { value } => {
                stream
                    .write_all(RESP::Bulk(value.into_bytes()).to_string().as_bytes())
                    .await?;
            }
            commands::Command::Set(SetArgs {
                key,
                value,
                expiry_mode,
            }) => {
                let expire_at = expiry_mode.map(|ex_mod| {
                    Instant::now()
                        + match ex_mod {
                            commands::Expiry::Ex { seconds } => Duration::from_secs(seconds),
                            commands::Expiry::Px { millis } => Duration::from_millis(millis),
                        }
                });
                cache.set(key, value, expire_at).await;
                stream.write_all(b"+OK\r\n").await?;
            }
            commands::Command::Get { key } => {
                let cached_value = cache.get(key).await;
                match cached_value {
                    None => stream.write_all(RESP::NULL_BULK.as_bytes()).await,
                    Some(v) => stream.write_all(RESP::Bulk(v).to_string().as_bytes()).await,
                }?;
            }
            commands::Command::Rpush { list_key, values } => {
                let num_elems = cache.r_push(list_key, values).await?;
                stream
                    .write_all(RESP::Int(num_elems as i64).to_string().as_bytes())
                    .await?;
            }
            commands::Command::Lpush { list_key, values } => {
                let num_elems = cache.l_push(list_key, values).await?;
                stream
                    .write_all(RESP::Int(num_elems as i64).to_string().as_bytes())
                    .await?;
            }
            commands::Command::Lrange {
                list_key,
                start,
                end,
            } => {
                let res = cache.l_range(list_key, start, end).await?;
                match res {
                    Some(res) => {
                        stream
                            .write_all(
                                RESP::Array(res.into_iter().map(|e| RESP::Bulk(e)).collect())
                                    .to_string()
                                    .as_bytes(),
                            )
                            .await?
                    }
                    None => {
                        stream
                            .write_all(RESP::empty_array().to_string().as_bytes())
                            .await?
                    }
                };
            }
            commands::Command::Llen { list_key } => {
                let num_elems = cache.l_len(list_key).await.unwrap_or(0);
                stream
                    .write_all(RESP::Int(num_elems as i64).to_string().as_bytes())
                    .await?;
            }
            commands::Command::Lpop {
                list_key,
                num_elems,
            } => {
                let poped = cache.l_pop(list_key, num_elems.unwrap_or(1)).await?;
                match poped {
                    Some(poped) => {
                        if poped.len() == 1 {
                            // guaranteed to have one value, this way we don't clone
                            let val = poped.into_iter().next().unwrap();
                            stream
                                .write_all(RESP::Bulk(val).to_string().as_bytes())
                                .await?
                        } else {
                            let val =
                                RESP::Array(poped.into_iter().map(|e| RESP::Bulk(e)).collect());
                            stream.write_all(val.to_string().as_bytes()).await?;
                        }
                    }
                    None => {
                        stream
                            .write_all(RESP::empty_array().to_string().as_bytes())
                            .await?
                    }
                }
            }
        }
    }
}
