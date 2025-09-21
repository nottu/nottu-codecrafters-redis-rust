use clap::{Args, Parser, Subcommand};

#[derive(Debug, Parser)]
pub struct RedisCli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    #[command(alias = "INFO")]
    Info { info: String },
    #[command(alias = "PING")]
    Ping,
    #[command(alias = "ECHO")]
    Echo { value: String },
    #[command(alias = "SET")]
    Set(SetArgs),
    #[command(alias = "GET")]
    Get { key: String },
    #[command(alias = "INCR")]
    Incr { key: String },
    #[command(alias = "RPUSH")]
    Rpush {
        list_key: String,
        values: Vec<String>,
    },
    #[command(alias = "LPUSH")]
    Lpush {
        list_key: String,
        values: Vec<String>,
    },
    #[command(alias = "LRANGE")]
    Lrange {
        list_key: String,
        #[arg(allow_hyphen_values = true)]
        start: i64,
        #[arg(allow_hyphen_values = true)]
        end: i64,
    },
    #[command(alias = "LLEN")]
    Llen { list_key: String },
    #[command(alias = "LPOP")]
    Lpop {
        list_key: String,
        num_elems: Option<usize>,
    },
    #[command(alias = "BLPOP")]
    Blpop { list_key: String, time_out: f64 },
    #[command(alias = "TYPE")]
    Type { key: String },
    #[command(alias = "XADD")]
    Xadd {
        key: String,
        stream_id: String,
        #[arg(required = true)]
        data: Vec<String>,
    },
    #[command(alias = "XRANGE")]
    Xrange {
        key: String,
        lower_bound: String,
        upper_bound: String,
    },
    #[command(alias = "XREAD")]
    Xread { args: Vec<String> },
    #[command(alias = "MULTI")]
    Multi,
    #[command(alias = "EXEC")]
    Exec,
    #[command(alias = "DISCARD")]
    Discard,
}

pub fn parse_xread_args(mut args: Vec<String>) -> anyhow::Result<XreadArgs> {
    if args.len() < 3 {
        anyhow::bail!("Not enough args for xread {args:?}")
    };
    //
    let block = if args[0] == "block" {
        // next sould be a decimal value
        let block_time = args[1]
            .parse()
            .map_err(|_e| anyhow::anyhow!("Not a valid block time"))?;
        args = args.split_off(2);
        Some(block_time)
    } else {
        None
    };
    if args.len() < 3 {
        anyhow::bail!("Not enough args for xread {args:?}")
    };
    if args[0] != "streams" {
        anyhow::bail!("Expected `streams` keyword");
    }
    args = args.split_off(1);
    if args.len() % 2 != 0 {
        anyhow::bail!("Expected even number of args {args:?}");
    }
    let lower_bounds = args.split_off(args.len() / 2);
    let entry_keys = args;
    Ok(XreadArgs {
        block,
        keys: entry_keys,
        streams: lower_bounds,
    })
}

#[derive(Debug, Clone)]
pub struct XreadArgs {
    pub block: Option<u64>,
    pub keys: Vec<String>,
    pub streams: Vec<String>,
}

#[derive(Args, Debug)]
pub struct SetArgs {
    pub key: String,
    pub value: String,
    #[command(subcommand)]
    pub expiry_mode: Option<Expiry>,
}

#[derive(Debug, Parser, PartialEq, Eq)]
pub enum Expiry {
    // Seconds
    Ex { seconds: u64 },
    // Millis
    Px { millis: u64 },
}

#[cfg(test)]
mod command_test {
    use clap::Parser;

    use crate::{
        cli_commands::{parse_xread_args, Command, Expiry, RedisCli},
        resp::Frame,
    };

    #[test]
    fn test_simple_set_command() {
        let args = ["prog", "SET", "foo", "bar"];

        let set_cmd = RedisCli::parse_from(&args);

        if let Command::Set(set_args) = set_cmd.command {
            assert_eq!(set_args.key, "foo");
            assert_eq!(set_args.value, "bar");
        } else {
            eprint!("Command is not SET");
            assert!(false);
        }
    }
    #[test]
    fn test_px_set_command() {
        let args = ["prog", "SET", "foo", "bar", "px", "100"];

        let set_cmd = RedisCli::parse_from(&args);

        if let Command::Set(set_args) = set_cmd.command {
            assert_eq!(set_args.key, "foo");
            assert_eq!(set_args.value, "bar");
            assert_eq!(set_args.expiry_mode, Some(Expiry::Px { millis: 100 }));
        } else {
            eprint!("Command is not SET");
            assert!(false);
        }
    }
    #[test]
    fn test_parse_from_resp() {
        let args = vec![
            Frame::SimpleString("redis-cli".to_string()),
            Frame::SimpleString("SET".to_string()),
            Frame::SimpleString("banana".to_string()),
            Frame::SimpleString("orange".to_string()),
        ];

        let set_cmd = RedisCli::parse_from(args);

        if let Command::Set(set_args) = set_cmd.command {
            assert_eq!(set_args.key, "banana");
            assert_eq!(set_args.value, "orange");
        } else {
            eprint!("Command is not SET");
            assert!(false);
        }
    }
    #[test]
    fn test_xread() {
        let args = [
            "prog", "XREAD", "block", "100", "streams", "some_key", "0-1",
        ];

        let Command::Xread { args: x_read_args } = RedisCli::parse_from(&args).command else {
            unreachable!("expected xread")
        };
        dbg!(&x_read_args);

        let parsed_xread = parse_xread_args(x_read_args).expect("expected valid xread");
        assert_eq!(Some(100), parsed_xread.block);

        assert_eq!(vec!["some_key".to_string()], parsed_xread.keys);
        assert_eq!(vec!["0-1".to_string()], parsed_xread.streams);
    }
}
