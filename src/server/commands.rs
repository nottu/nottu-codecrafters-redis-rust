use tokio::time::Instant;

// TODO: Idially this should all be parsed from the cli args directly to avoid duplicate code
// with cli_commands.rs
// if-change then-change: cli_commands::Commands
// TODO: can we replace the String with &str so this is also Copy ?
#[derive(Debug, Clone)]
pub enum DataCommand {
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

impl DataCommand {
    pub fn is_write(&self) -> bool {
        match self {
            Self::Set {
                key: _,
                value: _,
                expire_at: _,
            } => true,
            Self::Increment { key: _ } => true,
            Self::ListAppend {
                list_key: _,
                values: _,
            } => true,
            Self::ListPrepend {
                list_key: _,
                values: _,
            } => true,
            Self::ListPop {
                list_key: _,
                num_elems: _,
            } => true,
            // TODO: Optimize blocking list pop...
            Self::BlockingListPop {
                list_key: _,
                timeout: _,
            } => true,
            Self::StreamAdd {
                key: _,
                stream_id: _,
                data: _,
            } => true,
            // TODO: Transactions shouldn't be write, instead once executed we should send over write commands
            Self::StartTransaction | Self::ExecuteTransaction | Self::DiscardTransaction => true,
            _ => false,
        }
    }
}

#[derive(Debug)]
pub enum ConnectionCommand {
    Ping,
    Echo { value: String },
    Info { info: String },
}

#[derive(Debug)]
pub enum ReplicaCommand {
    // TODO: the replica commands have arguments that we should handle
    ReplicateListeningPort,
    ReplicateCapabilities,
    ReplicateSycn { master_id: String, offset: i64 },
}

#[derive(Debug)]
pub enum Command {
    Connection(ConnectionCommand),
    Replica(ReplicaCommand),
    Data(DataCommand),
}

impl From<DataCommand> for Command {
    fn from(value: DataCommand) -> Self {
        Self::Data(value)
    }
}

impl From<ConnectionCommand> for Command {
    fn from(value: ConnectionCommand) -> Self {
        Self::Connection(value)
    }
}

impl From<ReplicaCommand> for Command {
    fn from(value: ReplicaCommand) -> Self {
        Self::Replica(value)
    }
}
