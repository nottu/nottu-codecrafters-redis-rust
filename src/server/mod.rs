use std::{collections::VecDeque, net::SocketAddr, path::Path, sync::Arc};

use tokio::{fs::File, io::AsyncReadExt, net::TcpStream, sync::Mutex};

use crate::{
    resp::Frame,
    server::{
        commands::DataCommand,
        connection::Connection,
        db::{Db, GuardedDb},
    },
};

pub mod cli_commands;
pub mod commands;
pub mod connection;
pub mod db;

#[derive(Debug)]
pub struct Server {
    mode: Mode,
    db: Db,
    transaction_op: Option<TransactionOp>,
    replicas: Arc<Mutex<Vec<ReplicaConnection>>>,
}

#[derive(Debug)]
pub struct ReplicaConnection {
    // TODO: add capabilities and listening port info...
    connection: Connection,
}

impl Server {
    const EMTPY_RDB: &'static str = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";
    pub fn new() -> Self {
        use std::fs::File;
        use std::io::Write;
        // create an empty rdb file
        let bytes = hex::decode(Self::EMTPY_RDB).expect("msg");
        let mut file = File::create("rdb.rdb").expect("msg");
        file.write_all(&bytes).expect("msg");
        Self {
            mode: Mode::Master {
                // TODO: Generate random ID instead
                id: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string(),
                offset: 0,
            },
            db: Db::new(),
            transaction_op: None,
            replicas: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn get_id(&self) -> Option<String> {
        match &self.mode {
            Mode::Master { id, offset: _ } => Some(id.clone()),
            Mode::Slave {
                master_id: _,
                master_connection: _,
                offset: _,
            } => None,
        }
    }

    pub async fn replicate(master: String, listening_port: &str) -> anyhow::Result<Self> {
        let (master_id, offset, connection) =
            Self::replication_handshake(master, listening_port).await?;
        eprintln!("Handshake completed!");
        let replica = Self {
            mode: Mode::Slave {
                master_id,
                offset,
                master_connection: connection.get_peer_addr(),
            },
            db: Db::new(),
            transaction_op: None,
            // is this needed?
            replicas: Arc::new(Mutex::new(Vec::new())),
        };
        eprintln!("Listening to Master");
        replica.start_connection(connection)?;
        Ok(replica)
    }

    async fn replication_handshake(
        master: String,
        listening_port: &str,
    ) -> anyhow::Result<(String, usize, Connection)> {
        let mut connection = Connection::new(TcpStream::connect(master.replace(" ", ":")).await?);
        // Ping master
        {
            eprintln!("replica_handshake: PING");
            connection
                .write_frame(&Frame::Array(vec![Frame::SimpleString("PING".to_string())]))
                .await?;
            connection.flush().await?;
            let resp = connection.read_frame().await?;
            match resp {
                Frame::SimpleString(res) if res == "PONG" => dbg!("Got PONG response!"),
                Frame::SimpleString(_) => anyhow::bail!("Expected Simple String PONG"),
                _ => anyhow::bail!("Expected simple string as reponse"),
            };
        }
        // Send listening port
        {
            eprintln!("replica_handshake: REPLCONF listening-port");
            connection
                .write_frame(&Frame::Array(vec![
                    Frame::bulk_from_str("REPLCONF"),
                    Frame::bulk_from_str("listening-port"),
                    Frame::bulk_from_str(listening_port),
                ]))
                .await?;
            connection.flush().await?;
            let resp = connection.read_frame().await?;
            match resp {
                Frame::SimpleString(res) if res == "OK" => dbg!("Got OK response!"),
                Frame::SimpleString(_) => anyhow::bail!("Expected Simple String PONG"),
                _ => anyhow::bail!("Expected simple string as reponse"),
            };
        }
        // Send capabilities info
        {
            eprintln!("replica_handshake: REPLCONF capa");
            connection
                .write_frame(&Frame::Array(vec![
                    Frame::bulk_from_str("REPLCONF"),
                    Frame::bulk_from_str("capa"),
                    Frame::bulk_from_str("psync2"),
                ]))
                .await?;
            connection.flush().await?;
            let resp = connection.read_frame().await?;
            match resp {
                Frame::SimpleString(res) if res == "OK" => dbg!("Got OK response!"),
                Frame::SimpleString(_) => anyhow::bail!("Expected Simple String OK"),
                _ => anyhow::bail!("Expected simple string as reponse"),
            };
        }

        // Send PSYNC command
        let (master_id, offset) = {
            eprintln!("replica_handshake: PSYNC");
            connection
                .write_frame(&Frame::Array(vec![
                    Frame::bulk_from_str("PSYNC"),
                    Frame::bulk_from_str("?"),
                    Frame::bulk_from_str("-1"),
                ]))
                .await?;
            connection.flush().await?;
            match connection.read_frame().await? {
                Frame::SimpleString(res) => {
                    let mut res = res.split(" ");
                    // should have 3 items FULLRESYC <REPL_ID> <OFFSET>
                    let Some(resync) = res.next() else {
                        anyhow::bail!("Expected a FULLRESYNC");
                    };
                    if resync != "FULLRESYNC" {
                        anyhow::bail!("Expected a FULLRESYNC");
                    }

                    let Some(master_id) = res.next() else {
                        anyhow::bail!("Expected <REPL_ID>")
                    };

                    let Some(offset) = res.next() else {
                        anyhow::bail!("Expected <OFFSET>")
                    };

                    let offset: usize = offset
                        .parse()
                        .map_err(|_| anyhow::anyhow!("Expected a numeric offset"))?;

                    (master_id.to_string(), offset)
                }
                _ => anyhow::bail!("Expected simple string as reponse"),
            }
            // ("?".to_string(), 0)
        };

        // Read RDB
        {
            eprintln!("Reading RDB File");
            let _rdb = connection.read_rdb().await?;
        }

        Ok((master_id, offset, connection))
    }

    fn get_replication_info(&self) -> String {
        match &self.mode {
            Mode::Slave {
                master_id,
                offset,
                master_connection: _,
            } => format!("role:slave\nmaster_replid:{master_id}\nmaster_repl_offset:{offset}"),
            Mode::Master { id, offset } => {
                format!("role:master\nmaster_replid:{id}\nmaster_repl_offset:{offset}")
            }
        }
    }

    async fn get_rdb_file(&self) -> anyhow::Result<Vec<u8>> {
        // TODO: make file path configurable
        let path = Path::new("rdb.rdb");
        let mut file = File::open(&path).await?;

        let mut buf = Vec::new();
        file.read_to_end(&mut buf).await?;

        Ok(buf)
    }

    async fn execute_command(&mut self, command: DataCommand, original_command: &Frame) -> Frame {
        // Execute all blocking commands here, and all non blocking should be re-routed
        let is_write_command = command.is_write();
        let out = match command {
            DataCommand::StartTransaction => {
                self.transaction_op = Some(TransactionOp {
                    queue: VecDeque::new(),
                });
                Frame::ok()
            }
            DataCommand::ExecuteTransaction => match self.transaction_op.take() {
                Some(transaction_op) => {
                    let mut resuls = vec![];
                    let mut guarded_db = self.db.get_guarded().await;
                    for command in transaction_op.queue {
                        let cmd_res =
                            Self::execute_non_blocking_command(command, &mut guarded_db).await;
                        resuls.push(cmd_res);
                    }
                    Frame::Array(resuls)
                }
                None => Frame::Error("ERR EXEC without MULTI".to_string()),
            },
            DataCommand::DiscardTransaction => match self.transaction_op.take() {
                Some(_) => Frame::ok(),
                None => Frame::Error("ERR DISCARD without MULTI".to_string()),
            },
            DataCommand::BlockingListPop {
                list_key,
                timeout: time_out,
            } => match self.db.blocking_list_pop(&list_key, &time_out).await {
                Ok(popped) => match popped {
                    Some(popped) => {
                        let list_key_frame = Frame::bulk_from_string(list_key);
                        let popped_frame = Frame::Bulk(popped);
                        Frame::Array(vec![list_key_frame, popped_frame])
                    }
                    None => Frame::NullArray,
                },
                Err(e) => Frame::Error(format!(
                    "Err BLPOP failed with internal error: {}",
                    e.to_string()
                )),
            },
            DataCommand::BlockingStreamRead {
                block_millis,
                key,
                stream,
            } => match self.db.blocking_x_read(block_millis, key, stream).await {
                Ok(frame) if frame == Frame::NullArray => frame,
                Ok(frame) => Frame::Array(vec![frame]),
                Err(e) => Frame::Error(format!(
                    "Err XREAD failed with internal error: {}",
                    e.to_string()
                )),
            },
            _ => match &mut self.transaction_op {
                Some(transaction_op) => {
                    transaction_op.queue.push_back(command);
                    Frame::SimpleString("QUEUED".to_string())
                }
                None => {
                    let mut guarded_db = self.db.get_guarded().await;
                    Self::execute_non_blocking_command(command, &mut guarded_db).await
                }
            },
        };

        // TODO: would this be better with a channel?
        if is_write_command && !out.is_err() {
            // Clone command and send it to all replicas...
            // TODO: Make command Copy!
            let mut replicas = self.replicas.lock().await;
            for replica in replicas.iter_mut() {
                if let Err(e) =
                    Self::forward_command(&mut replica.connection, original_command).await
                {
                    eprintln!("Failed to forward to replica {replica:?} with error: {e}");
                }
            }
        }
        out
    }

    async fn forward_command(connection: &mut Connection, command: &Frame) -> anyhow::Result<()> {
        eprintln!(
            "Forwarding command {command:?} to {}",
            connection.get_peer_addr()
        );
        connection.write_frame(command).await?;
        connection.flush().await?;
        Ok(())
    }

    async fn execute_non_blocking_command<'a>(
        command: DataCommand,
        db: &mut GuardedDb<'a>,
    ) -> Frame {
        match command {
            DataCommand::Set {
                key,
                value,
                expire_at,
            } => {
                db.set(key, value, expire_at).await;
                Frame::ok()
            }
            DataCommand::Get { key } => match db.get(key).await {
                Some(val) => Frame::Bulk(val),
                None => Frame::NullBulk,
            },
            DataCommand::Increment { key } => match db.increment(key).await {
                Ok(val) => Frame::Int(val as i64),
                Err(err) => Frame::Error(err.to_string()),
            },
            DataCommand::ListAppend { list_key, values } => {
                match db.list_append(list_key, values).await {
                    Ok(num_elems) => Frame::Int(num_elems as i64),
                    Err(e) => Frame::Error(e.to_string()),
                }
            }
            DataCommand::ListPrepend { list_key, values } => {
                match db.list_prepend(list_key, values).await {
                    Ok(num_elems) => Frame::Int(num_elems as i64),
                    Err(e) => Frame::Error(e.to_string()),
                }
            }
            DataCommand::ListRange {
                list_key,
                start,
                end,
            } => match db.list_range(list_key, start, end).await {
                Ok(res) => Frame::Array(res.into_iter().map(|v| Frame::Bulk(v)).collect()),
                Err(e) => Frame::Error(e.to_string()),
            },
            DataCommand::ListLen { list_key } => match db.list_len(list_key).await {
                Ok(len) => Frame::Int(len as i64),
                Err(e) => Frame::Error(e.to_string()),
            },
            DataCommand::ListPop {
                list_key,
                num_elems,
            } => match db.list_pop(list_key, num_elems.unwrap_or(1)).await {
                Ok(popped) => {
                    if popped.len() == 1 {
                        // guaranteed to have one value, this way we don't clone
                        let val = popped.into_iter().next().unwrap();
                        Frame::Bulk(val)
                    } else {
                        Frame::Array(popped.into_iter().map(|v| Frame::Bulk(v)).collect())
                    }
                }
                Err(e) => Frame::Error(e.to_string()),
            },
            DataCommand::BlockingListPop {
                list_key: _,
                timeout: _,
            } => {
                unreachable!()
            }
            DataCommand::EntryType { key } => {
                Frame::SimpleString(db.entry_type(key).await.to_string())
            }
            DataCommand::StreamAdd {
                key,
                stream_id,
                data,
            } => match db.stream_add(key, stream_id, &data).await {
                Ok(stream_id) => Frame::bulk_from_string(stream_id),
                Err(e) => Frame::Error(e.to_string()),
            },
            DataCommand::StreamRange {
                key,
                lower_bound,
                upper_bound,
            } => match db.stream_range(key, lower_bound, upper_bound).await {
                Ok(frame) => frame,
                Err(e) => Frame::Error(e.to_string()),
            },
            DataCommand::StreamRead { keys, streams } => {
                let mut results = Vec::with_capacity(keys.len());
                for (key, lower_bound) in keys.into_iter().zip(streams) {
                    let result = match db.stream_read(key, lower_bound).await {
                        Ok(res) if res == Frame::NullArray => continue,
                        Ok(res) => res,
                        Err(e) => Frame::Error(e.to_string()),
                    };
                    results.push(result);
                }

                if results.is_empty() {
                    Frame::NullArray
                } else {
                    Frame::Array(results)
                }
            }
            DataCommand::BlockingStreamRead {
                block_millis: _,
                key: _,
                stream: _,
            } => unreachable!(),
            DataCommand::StartTransaction
            | DataCommand::ExecuteTransaction
            | DataCommand::DiscardTransaction => {
                unreachable!()
            }
        }
    }

    pub fn start_connection(&self, connection: Connection) -> anyhow::Result<()> {
        let mut clone = self.clone();
        tokio::spawn(async move {
            if let Err(e) = clone.process_connection(connection).await {
                eprintln!("{e:?}");
            }
        });
        Ok(())
    }

    async fn process_connection(&mut self, mut connection: Connection) -> anyhow::Result<()> {
        eprintln!(
            "Starting with connection to {:?}",
            connection.get_peer_addr()
        );
        loop {
            let (command, original_command) = connection.read_command().await?;
            match command {
                commands::Command::Connection(command) => match command {
                    commands::ConnectionCommand::Ping => connection.write_pong_frame().await?,
                    commands::ConnectionCommand::Echo { value } => {
                        connection
                            .write_frame(&Frame::bulk_from_string(value))
                            .await?
                    }
                    commands::ConnectionCommand::Info { info } => match info.as_str() {
                        "replication" => {
                            connection
                                .write_frame(&Frame::bulk_from_string(self.get_replication_info()))
                                .await?
                        }
                        _ => {
                            connection
                                .write_simple_error(format!("Unknown Info command: {info}").as_str())
                                .await?
                        }
                    },
                },
                commands::Command::Data(command) => {
                    eprintln!("Executing Data command {command:?}, seding result to {}", connection.get_peer_addr());
                    connection
                        .write_frame(&self.execute_command(command, &original_command).await)
                        .await?
                }
                commands::Command::Replica(command) => match command {
                    commands::ReplicaCommand::ReplicateListeningPort => {
                        // log something...
                        eprintln!("Starting Replication");
                        connection.write_ok_frame().await?;
                        return self.start_replication(connection).await;
                    }
                    _ => connection.write_simple_error(
                        &format!("Unexpected replica command, expected `replconf listening-port <LISTENING_PORT>`")
                    ).await?
                }
            };
            connection.flush().await?;
        }
    }

    // This connection will only be used for replication, check that the handshake is ok
    // We won't ever listen to the connection as we don't expect it to send something (?)
    async fn start_replication(&mut self, mut connection: Connection) -> anyhow::Result<()> {
        // Check that next message is a REPLCONF capa
        {
            let (command, _) = connection.read_command().await?;
            // TODO: find a better way, kinda ugly check...
            let commands::Command::Replica(commands::ReplicaCommand::ReplicateCapabilities) =
                command
            else {
                connection
                    .write_simple_error(&format!(
                        "Expected a `replconf capa <CAPABILITIES>` command, got {command:?}"
                    ))
                    .await?;
                return Ok(());
            };
            connection.write_ok_frame().await?;
        }
        // Next message should be a PSYNC
        {
            let (command, _) = connection.read_command().await?;
            // TODO: find a better way, kinda ugly check...
            let commands::Command::Replica(commands::ReplicaCommand::ReplicateSycn {
                master_id,
                offset,
            }) = command
            else {
                connection
                    .write_simple_error(&format!(
                        "Expected a `replconf capa <CAPABILITIES>` command, got {command:?}"
                    ))
                    .await?;
                return Ok(());
            };
            eprintln!("{master_id} : {offset}");
            let Some(id) = self.get_id() else {
                connection
                    .write_simple_error("ERR not a master node")
                    .await?;
                return Ok(());
            };
            let frame = Frame::SimpleString(format!("FULLRESYNC {id} 0"));
            connection.write_frame(&frame).await?;
            connection.flush().await?;

            // Write the rdb file
            let rdb = self.get_rdb_file().await?;
            eprintln!("Writing rdb: {}", hex::encode(&rdb));
            connection.write_raw(&rdb).await?;
            connection.flush().await?;

            eprintln!("Adding connection to set of replicas");
            self.replicas
                .lock()
                .await
                .push(ReplicaConnection { connection });
        }
        Ok(())
    }
}

#[derive(Debug)]
struct TransactionOp {
    queue: VecDeque<DataCommand>,
}

impl Clone for Server {
    fn clone(&self) -> Self {
        debug_assert!(self.transaction_op.is_none());
        Self {
            mode: self.mode.clone(),
            db: self.db.clone(),
            transaction_op: None,
            replicas: self.replicas.clone(),
        }
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
enum Mode {
    Master {
        id: String,
        offset: usize,
    },
    Slave {
        master_id: String,
        offset: usize,
        master_connection: SocketAddr,
    },
}
