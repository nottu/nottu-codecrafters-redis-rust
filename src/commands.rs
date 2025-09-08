use clap::{Args, Parser, Subcommand};

#[derive(Debug, Parser)]
pub struct RedisCli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    #[command(alias = "PING")]
    Ping,
    #[command(alias = "ECHO")]
    Echo {
        value: String,
    },
    #[command(alias = "SET")]
    Set(SetArgs),
    #[command(alias = "GET")]
    Get {
        key: String,
    },
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
    Llen {
        list_key: String,
    },
    #[command(alias = "LPOP")]
    Lpop {
        list_key: String,
        num_elems: Option<usize>,
    },
    Close,
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
        commands::{Command, Expiry, RedisCli},
        resp::RESP,
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
            RESP::SimpleString("redis-cli".to_string()),
            RESP::SimpleString("SET".to_string()),
            RESP::SimpleString("banana".to_string()),
            RESP::SimpleString("orange".to_string()),
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
}
