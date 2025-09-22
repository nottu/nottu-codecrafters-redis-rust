use std::{collections::VecDeque, path::Path, sync::Arc};

use tokio::{fs::File, io::AsyncReadExt, net::TcpStream, sync::Mutex, time::Instant};

use crate::{
    connection::Connection,
    resp::Frame,
    server::db::{Db, GuardedDb},
};

pub mod db;

#[derive(Debug)]
pub struct Server {
    mode: Mode,
    db: Db,
    transaction_op: Option<TransactionOp>,
}

// TODO: Idially this should all be parsed from the cli args directly to avoid duplicate code
// with cli_commands.rs
// if-change then-change: cli_commands::Commands
#[derive(Debug)]
pub enum Command {
    Info {
        info: String,
    },
    /// SET
    Set {
        key: String,
        value: String,
        expire_at: Option<Instant>,
    },
    /// GET
    Get {
        key: String,
    },
    /// INCR
    Increment {
        key: String,
    },
    /// `[RPUSH]`
    ListAppend {
        list_key: String,
        values: Vec<String>,
    },
    /// `[LPUSH]`
    ListPrepend {
        list_key: String,
        values: Vec<String>,
    },
    /// `[LRANGE]`
    ListRange {
        list_key: String,
        start: i64,
        end: i64,
    },
    /// `[LLEN]`
    ListLen {
        list_key: String,
    },
    /// `[LPOP]`
    ListPop {
        list_key: String,
        num_elems: Option<usize>,
    },
    /// `[BLPOP]`
    BlockingListPop {
        list_key: String,
        timeout: Option<Instant>,
    },
    /// `[TYPE]`
    EntryType {
        key: String,
    },
    /// `[XADD]`
    StreamAdd {
        key: String,
        stream_id: String,
        data: Vec<String>,
    },
    /// `[XRANGE]`
    StreamRange {
        key: String,
        lower_bound: String,
        upper_bound: String,
    },
    /// `[XREAD]`
    StreamRead {
        keys: Vec<String>,
        streams: Vec<String>,
    },
    /// `[XREAD]` Blocking version of StreamRead. Only supports reading one stream at a time
    BlockingStreamRead {
        block_millis: u64,
        key: String,
        stream: String,
    },

    StartTransaction,

    ExecuteTransaction,

    DiscardTransaction,
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
        }
    }

    pub fn get_id(&self) -> Option<String> {
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
        let replica = Self {
            mode: Mode::Slave {
                master_id,
                offset,
                master_connection: Arc::new(Mutex::new(connection)),
            },
            db: Db::new(),
            transaction_op: None,
        };
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

        Ok((master_id, offset, connection))
    }

    pub fn get_replication_info(&self) -> String {
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

    pub async fn get_rdb_file(&self) -> anyhow::Result<Vec<u8>> {
        // TODO: make file path configurable
        let path = Path::new("rdb.rdb");
        let mut file = File::open(&path).await?;

        let mut buf = Vec::new();
        file.read_to_end(&mut buf).await?;

        Ok(buf)
    }

    pub async fn execute_command(&mut self, command: Command) -> Frame {
        // Execute all blocking commands here, and all non blocking should be re-routed
        match command {
            // Not blocking, but should not be callable inside multi (?)
            Command::Info { info } => match info.as_str() {
                "replication" => Frame::bulk_from_string(self.get_replication_info()),
                _ => Frame::Error("Unknown Info command {info}".to_string()),
            },
            Command::StartTransaction => {
                self.transaction_op = Some(TransactionOp {
                    queue: VecDeque::new(),
                });
                Frame::ok()
            }
            Command::ExecuteTransaction => match self.transaction_op.take() {
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
            Command::DiscardTransaction => match self.transaction_op.take() {
                Some(_) => Frame::ok(),
                None => Frame::Error("ERR DISCARD without MULTI".to_string()),
            },
            Command::BlockingListPop {
                list_key,
                timeout: time_out,
            } => match self.db.blocking_list_pop(list_key.clone(), time_out).await {
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
            Command::BlockingStreamRead {
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
        }
    }

    async fn execute_non_blocking_command<'a>(command: Command, db: &mut GuardedDb<'a>) -> Frame {
        match command {
            Command::Info { info: _ } => unreachable!(),
            Command::Set {
                key,
                value,
                expire_at,
            } => {
                db.set(key, value, expire_at).await;
                Frame::ok()
            }
            Command::Get { key } => match db.get(key).await {
                Some(val) => Frame::Bulk(val),
                None => Frame::NullBulk,
            },
            Command::Increment { key } => match db.increment(key).await {
                Ok(val) => Frame::Int(val as i64),
                Err(err) => Frame::Error(err.to_string()),
            },
            Command::ListAppend { list_key, values } => {
                match db.list_append(list_key, values).await {
                    Ok(num_elems) => Frame::Int(num_elems as i64),
                    Err(e) => Frame::Error(e.to_string()),
                }
            }
            Command::ListPrepend { list_key, values } => {
                match db.list_prepend(list_key, values).await {
                    Ok(num_elems) => Frame::Int(num_elems as i64),
                    Err(e) => Frame::Error(e.to_string()),
                }
            }
            Command::ListRange {
                list_key,
                start,
                end,
            } => match db.list_range(list_key, start, end).await {
                Ok(res) => Frame::Array(res.into_iter().map(|v| Frame::Bulk(v)).collect()),
                Err(e) => Frame::Error(e.to_string()),
            },
            Command::ListLen { list_key } => match db.list_len(list_key).await {
                Ok(len) => Frame::Int(len as i64),
                Err(e) => Frame::Error(e.to_string()),
            },
            Command::ListPop {
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
            Command::BlockingListPop {
                list_key: _,
                timeout: _,
            } => {
                unreachable!()
            }
            Command::EntryType { key } => Frame::SimpleString(db.entry_type(key).await.to_string()),
            Command::StreamAdd {
                key,
                stream_id,
                data,
            } => match db.stream_add(key, stream_id, &data).await {
                Ok(stream_id) => Frame::bulk_from_string(stream_id),
                Err(e) => Frame::Error(e.to_string()),
            },
            Command::StreamRange {
                key,
                lower_bound,
                upper_bound,
            } => match db.stream_range(key, lower_bound, upper_bound).await {
                Ok(frame) => frame,
                Err(e) => Frame::Error(e.to_string()),
            },
            Command::StreamRead { keys, streams } => {
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
            Command::BlockingStreamRead {
                block_millis: _,
                key: _,
                stream: _,
            } => unreachable!(),
            Command::StartTransaction
            | Command::ExecuteTransaction
            | Command::DiscardTransaction => {
                unreachable!()
            }
        }
    }
}

#[derive(Debug)]
struct TransactionOp {
    queue: VecDeque<Command>,
}

impl Clone for Server {
    fn clone(&self) -> Self {
        debug_assert!(self.transaction_op.is_none());
        Self {
            mode: self.mode.clone(),
            db: self.db.clone(),
            transaction_op: None,
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
        master_connection: Arc<Mutex<Connection>>,
    },
}
