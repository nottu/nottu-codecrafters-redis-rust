use std::time::Duration;

use tokio::{
    net::{TcpListener, TcpStream},
    time::Instant,
};

use crate::{commands::SetArgs, connection::Connection, db::Db, resp::Frame};

mod commands;
mod connection;
mod db;
mod resp;

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

async fn process_connection(stream: TcpStream, cache: Db) -> anyhow::Result<()> {
    let mut connection = Connection::new(stream);
    loop {
        let command = connection.read_command().await?;
        match command {
            commands::Command::Close => {
                return Ok(());
            }
            commands::Command::Ping => {
                connection.write_simple("PONG").await?;
            }
            commands::Command::Echo { value } => {
                connection.write_bytes(value.as_bytes()).await?;
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
                connection.write_simple("OK").await?;
            }
            commands::Command::Get { key } => {
                let cached_value = cache.get(key).await;
                match cached_value {
                    None => connection.write_nil().await?,
                    Some(v) => connection.write_bytes(&v).await?,
                };
            }
            commands::Command::Rpush { list_key, values } => {
                let num_elems = cache.r_push(list_key, values).await?;
                connection.write_int(num_elems as u64).await?;
            }
            commands::Command::Lpush { list_key, values } => {
                let num_elems = cache.l_push(list_key, values).await?;
                connection.write_int(num_elems as u64).await?;
            }
            commands::Command::Lrange {
                list_key,
                start,
                end,
            } => {
                let res = cache.l_range(list_key, start, end).await?.unwrap_or(vec![]);
                connection.write_array(&res).await?;
            }
            commands::Command::Llen { list_key } => {
                let num_elems = cache.l_len(list_key).await.unwrap_or(0);
                connection.write_int(num_elems as u64).await?;
            }
            commands::Command::Lpop {
                list_key,
                num_elems,
            } => {
                let popped = cache.l_pop(list_key, num_elems.unwrap_or(1)).await?;
                match popped {
                    Some(poped) => {
                        if poped.len() == 1 {
                            // guaranteed to have one value, this way we don't clone
                            let val = poped.into_iter().next().unwrap();
                            connection.write_bytes(&val).await?;
                        } else {
                            connection.write_array(&poped).await?;
                        }
                    }
                    None => {
                        connection.write_array(&[]).await?;
                    }
                }
            }
            commands::Command::Blpop { list_key, time_out } => {
                let timeout = if time_out == 0.0 {
                    None
                } else {
                    Some(Instant::now() + Duration::from_secs_f64(time_out))
                };
                eprint!("calling bl_popped");
                let popped = cache.bl_pop(list_key.clone(), timeout).await;
                eprintln!("bl_poped returned {popped:?}");
                match popped? {
                    Some(popped) => {
                        connection
                            .write_array(&[list_key.into_bytes(), popped])
                            .await?
                    }
                    None => connection.write_nil_array().await?,
                }
            }
            commands::Command::Type { key } => {
                let entry_type = cache.entry_type(key).await;
                connection.write_simple(entry_type).await?;
            }
            commands::Command::Xadd {
                key,
                stream_id,
                data,
            } => match cache.x_add(key, stream_id, &data).await {
                Ok(stream_id) => connection.write_bytes(stream_id.as_bytes()).await?,
                Err(err) => connection.write_simple_err(&err.to_string()).await?,
            },
            commands::Command::Xrange {
                key,
                lower_bound,
                upper_bound,
            } => {
                let data = cache.x_range(key, lower_bound, upper_bound).await?;
                connection.write_frame(data).await?;
            }
            commands::Command::Xread {
                streams,
                key,
                lower_bound,
            } => {
                if streams != "streams" {
                    connection
                        .write_simple_err(&format!("Expected streams keyword, found {streams}"))
                        .await?;
                    return Ok(());
                }
                let data = cache.x_read(key.clone(), lower_bound).await?;
                connection
                    .write_frame(Frame::Array(
                        [Frame::Array([Frame::buld_from_string(key), data].to_vec())].to_vec(),
                    ))
                    .await?;
            }
        }
        connection.flush().await?;
    }
}
