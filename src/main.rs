use std::{collections::VecDeque, time::Duration};

use tokio::{
    net::{TcpListener, TcpStream},
    time::Instant,
};

use crate::{
    commands::{parse_xread_args, Command, SetArgs, XreadArgs},
    connection::Connection,
    db::Db,
    resp::Frame,
};

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

async fn execute_atomic_command(command: Command, cache: &Db) -> anyhow::Result<Frame> {
    let res_frame: Frame = match command {
        commands::Command::Ping => Frame::pong(),
        commands::Command::Echo { value } => Frame::bulk_from_str(&value),
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
            Frame::ok()
        }
        commands::Command::Get { key } => match cache.get(key).await {
            None => Frame::NullBulk,
            Some(v) => Frame::Bulk(v),
        },
        commands::Command::Incr { key } => match cache.increment(key).await {
            Ok(val) => Frame::Int(val as i64),
            Err(err) => Frame::Error(err.to_string()),
        },
        commands::Command::Rpush { list_key, values } => {
            let num_elems = cache.r_push(list_key, values).await?;
            Frame::Int(num_elems as i64)
        }
        commands::Command::Lpush { list_key, values } => {
            let num_elems = cache.l_push(list_key, values).await?;
            Frame::Int(num_elems as i64)
        }
        commands::Command::Lrange {
            list_key,
            start,
            end,
        } => {
            let res = cache.l_range(list_key, start, end).await?;
            Frame::Array(res.into_iter().map(|v| Frame::Bulk(v)).collect())
        }
        commands::Command::Llen { list_key } => {
            let num_elems = cache.l_len(list_key).await?;
            Frame::Int(num_elems as i64)
        }
        commands::Command::Lpop {
            list_key,
            num_elems,
        } => {
            let popped = cache.l_pop(list_key, num_elems.unwrap_or(1)).await?;

            if popped.len() == 1 {
                // guaranteed to have one value, this way we don't clone
                let val = popped.into_iter().next().unwrap();
                Frame::Bulk(val)
            } else {
                Frame::Array(popped.into_iter().map(|v| Frame::Bulk(v)).collect())
            }
        }
        commands::Command::Type { key } => {
            let entry_type = cache.entry_type(key).await;
            Frame::SimpleString(entry_type.to_string())
        }
        commands::Command::Xadd {
            key,
            stream_id,
            data,
        } => match cache.x_add(key, stream_id, &data).await {
            Ok(stream_id) => Frame::Bulk(stream_id.into_bytes()),
            Err(err) => Frame::Error(err.to_string()),
        },
        commands::Command::Xrange {
            key,
            lower_bound,
            upper_bound,
        } => cache.x_range(key, lower_bound, upper_bound).await?,
        commands::Command::Xread { args } => {
            let xread_args = parse_xread_args(args)?;
            x_read(xread_args, &cache).await?
        }
        commands::Command::Blpop {
            list_key: _,
            time_out: _,
        } => {
            unreachable!("BLPOP is not an aotmic command")
        }
        commands::Command::Multi => {
            unreachable!("MULTI is not an atomic command")
        }
    };
    Ok(res_frame)
}

async fn x_read(xread_args: XreadArgs, cache: &Db) -> anyhow::Result<Frame> {
    // this could get big... might not be ideal to hold in memory
    let mut streams_data = Vec::with_capacity(xread_args.keys.len());

    // TODO: How should we handle multiple blocking xread?
    // Can/Should we send this async?
    for (key, lower_bound) in xread_args.keys.into_iter().zip(xread_args.streams) {
        let data = cache.x_read(xread_args.block, key, lower_bound).await?;
        if let Frame::NullArray = data {
            continue;
        }
        streams_data.push(data);
    }
    let res = if streams_data.is_empty() {
        Frame::NullArray
    } else {
        Frame::Array(streams_data)
    };
    Ok(res)
}

async fn process_connection(stream: TcpStream, cache: Db) -> anyhow::Result<()> {
    let mut connection = Connection::new(stream);
    let mut command_queue: Option<VecDeque<Command>> = None;
    loop {
        let command = connection.read_command().await?;

        let out_frame = match command {
            commands::Command::Multi => {
                command_queue = Some(VecDeque::new());
                Frame::ok()
            }
            commands::Command::Blpop { list_key, time_out } => {
                let timeout = if time_out == 0.0 {
                    None
                } else {
                    Some(Instant::now() + Duration::from_secs_f64(time_out))
                };
                let popped = cache.bl_pop(list_key.clone(), timeout).await;
                match popped? {
                    Some(popped) => {
                        let list_key_frame = Frame::Bulk(list_key.into_bytes());
                        let popped_frame = Frame::Bulk(popped);
                        Frame::Array(vec![list_key_frame, popped_frame])
                    }
                    None => Frame::NullArray,
                }
            }
            commands::Command::Xread { args } => {
                // TODO: avoid clone, maybe use an intermediate db_proxy?
                let xread_args = parse_xread_args(args.clone())?;
                if let Some(q) = &mut command_queue {
                    if xread_args.block.is_some() {
                        Frame::Error("Blocking XREAD not allowed in a transaction".to_string())
                    } else {
                        q.push_back(commands::Command::Xread { args });
                        Frame::SimpleString("QUEUED".to_string())
                    }
                } else {
                    x_read(xread_args, &cache).await?
                }
            }
            _ => {
                if let Some(q) = &mut command_queue {
                    q.push_back(command);
                    Frame::SimpleString("QUEUED".to_string())
                } else {
                    execute_atomic_command(command, &cache).await?
                }
            }
        };
        connection.write_frame(&out_frame).await?;
    }
}
