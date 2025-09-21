use std::time::Duration;

use tokio::{
    net::{TcpListener, TcpStream},
    time::Instant,
};

use crate::{
    cli_commands::{parse_xread_args, SetArgs, XreadArgs},
    connection::Connection,
    server::Server,
};

use clap::Parser;

mod cli_commands;
mod connection;
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

    let server = match cli.replicaof {
        None => Server::new(),
        Some(master) => {
            eprint!("Replicating {master}");
            Server::replicate(master)
                .await
                .expect("Expected to replicate")
        }
    };

    loop {
        let (stream, _addr) = listener.accept().await?;
        // Clone/Increase ref count out of spawn so it can be moved
        let server = server.clone();
        tokio::spawn(async move {
            if let Err(e) = process_connection(stream, server).await {
                eprintln!("{e:?}");
            }
        });
    }
}

async fn process_connection(stream: TcpStream, mut server: Server) -> anyhow::Result<()> {
    let mut connection = Connection::new(stream);
    loop {
        let cli_command = connection.read_command().await?;

        let out_frame = match cli_command {
            // Should PING and ECHO be done in "server"?
            cli_commands::Command::Ping => resp::Frame::SimpleString("PONG".to_string()),
            cli_commands::Command::Echo { value } => resp::Frame::Bulk(value.into_bytes()),
            _ => server.execute_command(translate_command(cli_command)).await,
        };
        connection.write_frame(&out_frame).await?;
    }
}

fn translate_command(cli_command: cli_commands::Command) -> server::Command {
    use cli_commands::Command as cli_command;
    use server::Command as server_command;

    match cli_command {
        cli_command::Info { info } => server_command::Info { info },
        cli_command::Set(SetArgs {
            key,
            value,
            expiry_mode,
        }) => {
            let expire_at = expiry_mode.map(|ex_mod| {
                Instant::now()
                    + match ex_mod {
                        cli_commands::Expiry::Ex { seconds } => Duration::from_secs(seconds),
                        cli_commands::Expiry::Px { millis } => Duration::from_millis(millis),
                    }
            });
            server_command::Set {
                key,
                value,
                expire_at,
            }
        }
        cli_command::Get { key } => server_command::Get { key },
        cli_command::Incr { key } => server_command::Increment { key },
        cli_command::Rpush { list_key, values } => server_command::ListAppend { list_key, values },
        cli_command::Lpush { list_key, values } => server_command::ListPrepend { list_key, values },
        cli_command::Lrange {
            list_key,
            start,
            end,
        } => server_command::ListRange {
            list_key,
            start,
            end,
        },
        cli_command::Llen { list_key } => server_command::ListLen { list_key },
        cli_command::Lpop {
            list_key,
            num_elems,
        } => server_command::ListPop {
            list_key,
            num_elems,
        },
        cli_command::Blpop { list_key, time_out } => {
            let timeout = if time_out == 0.0 {
                None
            } else {
                Some(Instant::now() + Duration::from_secs_f64(time_out))
            };
            server_command::BlockingListPop { list_key, timeout }
        }
        cli_command::Type { key } => server_command::EntryType { key },
        cli_command::Xadd {
            key,
            stream_id,
            data,
        } => server_command::StreamAdd {
            key,
            stream_id,
            data,
        },
        cli_command::Xrange {
            key,
            lower_bound,
            upper_bound,
        } => server_command::StreamRange {
            key,
            lower_bound,
            upper_bound,
        },
        cli_command::Xread { args } => {
            // TODO: send proper error
            let XreadArgs {
                block,
                keys,
                streams,
            } = parse_xread_args(args.clone()).expect("expected valid args");
            match block {
                Some(block_millis) => server_command::BlockingStreamRead {
                    block_millis,
                    key: keys.into_iter().next().unwrap(),
                    stream: streams.into_iter().next().unwrap(),
                },
                None => server_command::StreamRead { keys, streams },
            }
        }
        cli_command::Multi => server_command::StartTransaction,
        cli_command::Exec => server_command::ExecuteTransaction,
        cli_command::Discard => server_command::DiscardTransaction,
        _ => unreachable!(),
    }
}
