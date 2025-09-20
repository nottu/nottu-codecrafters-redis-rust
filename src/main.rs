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
    server::Server,
};

use clap::Parser;

mod commands;
mod connection;
mod db;
mod resp;
mod server;

#[derive(Debug, Parser)]
#[command(about = "A Redis server for CodeCrafters", version = "0.1")]
struct Cli {
    /// Port to listen on
    #[arg(short, long, default_value_t = 6379)]
    port: u16,

    #[arg(long)]
    replicaof: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let addr = format!("127.0.0.1:{}", cli.port);
    eprintln!("Listening on port {}", cli.port);

    let listener = TcpListener::bind(&addr).await?;

    let cache = Db::new();
    let server = match cli.replicaof {
        None => Server::new(),
        Some(master) => {
            eprint!("Replicating {master}");
            Server::replicate(&master)
        }
    };

    loop {
        let (stream, _addr) = listener.accept().await?;
        // Clone/Increase ref count out of spawn so it can be moved
        let data = cache.clone();
        let server = server.clone();
        tokio::spawn(async move {
            if let Err(e) = process_connection(stream, data, server).await {
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
            Ok(stream_id) => Frame::bulk_from_string(stream_id),
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
        _ => {
            unreachable!("{command:?}")
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

async fn process_connection(stream: TcpStream, cache: Db, server: Server) -> anyhow::Result<()> {
    let mut connection = Connection::new(stream);
    let mut command_queue: Option<VecDeque<Command>> = None;
    loop {
        let command = connection.read_command().await?;

        let out_frame = match command {
            Command::Info { info } => match info.as_str() {
                "replication" => Frame::bulk_from_string(server.get_info()),
                _ => Frame::Error("Unknown Info command {info}".to_string()),
            },
            // TODO: Transactions are not atomic!
            commands::Command::Multi => {
                command_queue = Some(VecDeque::new());
                Frame::ok()
            }
            Command::Exec => match command_queue {
                Some(commands) => {
                    let mut resuls = vec![];
                    for cmd in commands {
                        let cmd_res = execute_atomic_command(cmd, &cache).await?;
                        resuls.push(cmd_res);
                    }
                    command_queue = None;
                    Frame::Array(resuls)
                }
                None => Frame::Error("ERR EXEC without MULTI".to_string()),
            },
            Command::Discard => match command_queue {
                Some(_) => {
                    command_queue = None;
                    Frame::ok()
                }
                None => Frame::Error("ERR DISCARD without MULTI".to_string()),
            },
            commands::Command::Blpop { list_key, time_out } => {
                let timeout = if time_out == 0.0 {
                    None
                } else {
                    Some(Instant::now() + Duration::from_secs_f64(time_out))
                };
                let popped = cache.bl_pop(list_key.clone(), timeout).await;
                match popped? {
                    Some(popped) => {
                        let list_key_frame = Frame::bulk_from_string(list_key);
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
