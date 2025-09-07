use std::{collections::HashMap, sync::Arc, time::Duration};

use clap::Parser;
use codecrafters_redis::RESP;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Mutex,
    time::Instant,
};

use crate::commands::RedisCli;

mod commands;

#[derive(Debug)]
struct CacheEntry {
    value: RESP,
    expire_at: Option<Instant>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379").await?;

    // basic shared cache
    // TODO: Periodically remove expired entries
    // From the [resis docs](https://redis.io/docs/latest/commands/expire/#how-redis-expires-keys)
    // A key is passively expired simply when some client tries to access it, and the key is found to be timed out.
    // [..] there are expired keys that will never be accessed again.
    // These keys should be expired anyway, so periodically Redis tests a few keys at random among keys with an expire set.
    // All the keys that are already expired are deleted from the keyspace.
    let data: Arc<Mutex<HashMap<String, CacheEntry>>> = Arc::new(Mutex::new(HashMap::new()));

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
    cache: Arc<Mutex<HashMap<String, CacheEntry>>>,
) -> anyhow::Result<()> {
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
            commands::Command::Set(set_args) => {
                let expire_at = set_args.expiry_mode.map(|ex_mod| {
                    Instant::now()
                        + match ex_mod {
                            commands::Expiry::Ex { seconds } => Duration::from_secs(seconds),
                            commands::Expiry::Px { millis } => Duration::from_millis(millis),
                        }
                });
                {
                    let mut locked_cache = cache.lock().await;
                    let entry = CacheEntry {
                        // The set command only stores bulk strings
                        value: RESP::Bulk(set_args.value.into_bytes()),
                        expire_at,
                    };
                    eprintln!("Storing entry {entry:?}, instant_now: {:?}", Instant::now());
                    locked_cache.insert(set_args.key, entry);
                }
                stream.write_all(b"+OK\r\n").await?;
            }
            commands::Command::Get { key } => {
                let mut locked_cache = cache.lock().await;
                let cached_value = {
                    if let Some(cached_val) = locked_cache.get(&key) {
                        match cached_val.expire_at {
                            None => Some(&cached_val.value),
                            Some(expiry) => {
                                if Instant::now() >= expiry {
                                    eprintln!("Key is expired");
                                    locked_cache.remove(&key);
                                    None
                                } else {
                                    Some(&cached_val.value)
                                }
                            }
                        }
                    } else {
                        None
                    }
                };
                match cached_value {
                    None => stream.write_all(RESP::NULL_BULK.as_bytes()).await,
                    Some(v) => stream.write_all(v.to_string().as_bytes()).await,
                }?;
            }
            commands::Command::Rpush { list_key, values } => {
                let num_elems = {
                    let mut locked_cache = cache.lock().await;
                    let entry = locked_cache.entry(list_key).or_insert(CacheEntry {
                        value: RESP::empty_array(),
                        expire_at: None,
                    });
                    for value in values {
                        let _ = entry.value.push(value)?;
                    }
                    entry.value.len()?
                };
                stream
                    .write_all(RESP::Int(num_elems as i64).to_string().as_bytes())
                    .await?;
            }
            commands::Command::Lpush { list_key, values } => {
                let num_elems = {
                    let mut locked_cache = cache.lock().await;
                    let entry = locked_cache.entry(list_key).or_insert(CacheEntry {
                        value: RESP::empty_array(),
                        expire_at: None,
                    });
                    for value in values {
                        let _ = entry.value.prepend(value)?;
                    }
                    entry.value.len()?
                };
                stream
                    .write_all(RESP::Int(num_elems as i64).to_string().as_bytes())
                    .await?;
            }
            commands::Command::Lrange {
                list_key,
                start,
                end,
            } => {
                let locked_cache = cache.lock().await;
                let res = locked_cache
                    .get(&list_key)
                    .map(|entry| entry.value.range(start, end))
                    .transpose()?
                    .unwrap_or(RESP::empty_array());
                stream.write_all(res.to_string().as_bytes()).await?;
            }
            commands::Command::Llen { list_key } => {
                let locked_cache = cache.lock().await;
                let num_elems = locked_cache
                    .get(&list_key)
                    .map(|entry| entry.value.len())
                    .transpose()?
                    .unwrap_or(0);
                stream
                    .write_all(RESP::Int(num_elems as i64).to_string().as_bytes())
                    .await?;
            }
        }
    }
}
