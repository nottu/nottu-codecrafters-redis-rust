use std::collections::VecDeque;

use tokio::time::Instant;

use crate::{
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
    pub fn new() -> Self {
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
    pub fn replicate(_master: &str) -> Self {
        Self {
            mode: Mode::Slave,
            db: Db::new(),
            transaction_op: None,
        }
    }
    pub fn get_replication_info(&self) -> String {
        match &self.mode {
            Mode::Slave => format!("role:slave"),
            Mode::Master { id, offset } => {
                format!("role:master\nmaster_replid:{id}\nmaster_repl_offset:{offset}")
            }
        }
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

#[derive(Debug, Clone)]
enum Mode {
    Master { id: String, offset: usize },
    Slave,
}
