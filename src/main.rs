use std::{collections::HashMap, sync::Arc, time::Duration};

use codecrafters_redis::RESP;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Mutex,
    time::Instant,
};

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
                // there's at least two more values
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
                let value = args[1].clone();
                // TODO: properly parse all SET args...
                // https://redis.io/docs/latest/commands/set/
                // For now assume, args[2] is PX
                let expire_at = args
                    .get(3)
                    .map(|ttl| {
                        let ttl = match ttl {
                            RESP::Int(v) => *v as u64,
                            RESP::SimpleString(s) => s.parse()?,
                            RESP::Bulk(s) => String::from_utf8(s.clone())?.parse()?,
                            _ => {
                                return Err(anyhow::anyhow!(
                                    "ttl cannot be interpreted as an itn, val {ttl:?}"
                                ))
                            }
                        };
                        eprintln!("TTL: {ttl}");
                        Ok(Instant::now() + Duration::from_millis(ttl))
                    })
                    .transpose()?; // <- flip from Option<Result<_>> to Result<Option<_>>
                let entry = CacheEntry { value, expire_at };
                eprintln!("Storing entry {entry:?}, instant_now: {:?}", Instant::now());
                locked_cache.insert(key.clone(), entry);
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
                let mut locked_cache = cache.lock().await;
                let val = locked_cache.get(key);
                let to_write = match val {
                    Some(v) => match v.expire_at {
                        None => v.value.to_string(),
                        Some(instant) => {
                            eprintln!("Checking if expired");
                            if Instant::now() >= instant {
                                eprintln!("Key is expired");
                                locked_cache.remove(key);
                                RESP::NULL_BULK.to_string()
                            } else {
                                v.value.to_string()
                            }
                        }
                    },
                    None => RESP::NULL_BULK.to_string(),
                };
                eprintln!("sending message: {to_write}");
                stream.write_all(to_write.as_bytes()).await?;
            }
            _ => return Err(anyhow::anyhow!("Unknown command {command}")),
        }
    }
}
