use std::{fmt, io::Cursor, net::SocketAddr, time::Duration};

use anyhow::bail;
use bytes::{Buf, BytesMut};
use clap::Parser;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufWriter},
    net::TcpStream,
    time::Instant,
};

use crate::{
    resp::Frame,
    server::cli_commands::{self, parse_xread_args, RedisCli, XreadArgs},
    server::{self, cli_commands::SetArgs, commands},
};

pub struct Connection {
    stream: BufWriter<TcpStream>,
    buf: BytesMut,
    bytes_read: usize,
}

impl fmt::Debug for Connection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Connection")
            .field("stream", &self.stream)
            .field("buf", &"[u8; BUF_SIZE]")
            .finish()
    }
}

/// Writed values in an encoded manner
/// Assumes that we can only return strings, [u8] slices and lists of [u8] slices
impl Connection {
    // Default to a 4 KB read buffer, might need to be bigger
    const BUF_SIZE: usize = 1024 * 4;
    pub fn new(stream: TcpStream) -> Self {
        Self {
            stream: BufWriter::new(stream),
            buf: BytesMut::with_capacity(Self::BUF_SIZE),
            bytes_read: 0,
        }
    }

    pub fn reset_count(&mut self) {
        self.bytes_read = 0;
    }

    pub fn bytes_read(&self) -> usize {
        self.bytes_read
    }

    pub async fn read_command(&mut self) -> anyhow::Result<(server::commands::Command, Frame)> {
        let data = self.read_frame().await?;
        let Frame::Array(command_args) = &data else {
            bail!("expected an array with command and arguments, got {data:?}")
        };
        let parsed_commands = RedisCli::try_parse_from(
            // clap wants the first arg to be the program name... we pre-pend a value to comply
            std::iter::once(&Frame::SimpleString("redis-cli".to_string())).chain(command_args),
        )?;
        Ok((translate_command(parsed_commands.command), data))
    }

    pub async fn read_frame(&mut self) -> anyhow::Result<Frame> {
        loop {
            if let Some(frame) = self.parse_frame() {
                return Ok(frame);
            }
            if self.stream.read_buf(&mut self.buf).await? == 0 {
                // Connection was closed!
                if self.buf.is_empty() {
                    anyhow::bail!("Connection closed");
                } else {
                    anyhow::bail!("Connection reset by peer")
                }
            }
        }
    }

    fn parse_frame(&mut self) -> Option<Frame> {
        let mut cursor = Cursor::new(&self.buf[..]);
        let frame = Frame::try_parse(&mut cursor);

        match frame {
            Ok(frame) => {
                let bytes_read = cursor.position() as usize;
                self.buf.advance(bytes_read);
                self.bytes_read += bytes_read;

                Some(frame)
            }
            Err(e) => {
                eprintln!(
                    "Could not parse frame! {e}, \n\tData: {:?}",
                    Self::escape_bytes(&self.buf[..])
                );
                None
            }
        }
    }

    fn escape_bytes(input: &[u8]) -> String {
        input
            .iter()
            .flat_map(|&b| std::ascii::escape_default(b))
            .map(|c| c as char)
            .collect()
    }

    pub async fn read_rdb(&mut self) -> anyhow::Result<Vec<u8>> {
        eprintln!("Reading RDB");
        loop {
            match self.parse_rdb() {
                Ok(rdb) => return Ok(rdb),
                Err(e) => {
                    if e.to_string().as_str() != "Not Enough Bytes" {
                        return Err(e);
                    }
                }
            }

            if self.stream.read_buf(&mut self.buf).await? == 0 {
                // Connection was closed!
                if self.buf.is_empty() {
                    anyhow::bail!("Connection closed");
                } else {
                    anyhow::bail!("Connection reset by peer")
                }
            }
        }
    }

    pub fn parse_rdb(&mut self) -> anyhow::Result<Vec<u8>> {
        let mut cursor = Cursor::new(&self.buf[..]);
        // let total = cursor.remaining();
        // rdb should be like:
        // $<length_of_file>\r\n<binary_contents_of_file>
        if cursor.remaining() < 4 {
            anyhow::bail!("Not Enough Bytes");
        }
        if cursor.get_u8() != b'$' {
            anyhow::bail!("Malfomed RDB transfer, expected $");
        }
        let len = Frame::get_decimal(&mut cursor)? as usize;
        let rdb = cursor.chunk()[..len].to_vec();

        let bytes_read = cursor.position() as usize + len;

        self.buf.advance(bytes_read);
        self.bytes_read += bytes_read;

        Ok(rdb)
    }

    async fn write_decimal(&mut self, val: i64) -> anyhow::Result<()> {
        use std::io::{Cursor, Write};

        let buf = [0u8; 20];
        let mut buf = Cursor::new(buf);
        write!(&mut buf, "{}", val)?;
        let pos = buf.position() as usize;

        self.stream.write_all(&buf.get_ref()[..pos]).await?;
        self.stream.write_all(b"\r\n").await?;
        Ok(())
    }

    pub async fn write_frame(&mut self, frame: &Frame) -> anyhow::Result<()> {
        // TODO avoid creating a new String...
        eprintln!("Writing Frame {frame:?}");
        match frame {
            Frame::Array(arr) => {
                self.stream.write_all(b"*").await?;
                self.write_decimal(arr.len() as i64).await?;
                for value in arr {
                    self.write_value(&value).await?;
                }
            }
            _ => self.write_value(frame).await?,
        }
        // self.stream.flush().await?;
        Ok(())
    }

    pub async fn write_ok_frame(&mut self) -> anyhow::Result<()> {
        self.stream.write_all(b"+OK\r\n").await?;
        self.stream.flush().await?;
        Ok(())
    }

    pub async fn write_pong_frame(&mut self) -> anyhow::Result<()> {
        self.stream.write_all(b"+PONG\r\n").await?;
        // self.stream.flush().await?;
        Ok(())
    }

    pub async fn write_simple_error(&mut self, err: &str) -> anyhow::Result<()> {
        self.stream.write_all(b"-").await?;
        self.stream.write_all(err.as_bytes()).await?;
        self.stream.write_all(b"\r\n").await?;
        // self.stream.flush().await?;
        Ok(())
    }

    // Similar to a write bulk string, but with out the carriage return at the end
    pub async fn write_raw(&mut self, src: &[u8]) -> anyhow::Result<()> {
        self.stream.write_all(b"$").await?;
        self.write_decimal(src.len() as i64).await?;
        self.stream.write_all(src).await?;
        // self.stream.flush().await?;
        Ok(())
    }

    pub async fn flush(&mut self) -> anyhow::Result<()> {
        self.stream.flush().await?;
        Ok(())
    }

    async fn write_value(&mut self, frame: &Frame) -> anyhow::Result<()> {
        match frame {
            Frame::Array(_) => {
                // we can't do recursive calls in an async context,
                // create a string for the whole array and write that
                self.stream.write_all(&frame.to_string().as_bytes()).await?;
            }
            Frame::NullArray => self.stream.write_all(b"*-1\r\n").await?,
            Frame::NullBulk => self.stream.write_all(b"$-1\r\n").await?,
            Frame::SimpleString(s) => {
                self.stream.write(b"+").await?;
                self.stream.write(s.as_bytes()).await?;
                self.stream.write(b"\r\n").await?;
            }
            Frame::Bulk(data) => {
                self.stream.write_all(b"$").await?;
                self.write_decimal(data.len() as i64).await?;
                self.stream.write(data).await?;
                self.stream.write(b"\r\n").await?;
            }
            Frame::Int(val) => {
                self.stream.write_all(b":").await?;
                self.write_decimal(*val).await?;
            }
            Frame::Error(err) => {
                self.stream.write_all(b"-").await?;
                self.stream.write_all(err.as_bytes()).await?;
                self.stream.write_all(b"\r\n").await?;
            }
        }
        Ok(())
    }
}

impl Connection {
    pub fn get_peer_addr(&self) -> SocketAddr {
        self.stream
            .get_ref()
            .peer_addr()
            .expect("Expected Peer Addr")
    }
}

fn translate_command(cli_command: cli_commands::Command) -> commands::Command {
    use crate::server::commands as server_command;
    use cli_commands::Command as cli_command;

    match cli_command {
        cli_command::Ping => server_command::ConnectionCommand::Ping.into(),
        cli_command::Echo { value } => server_command::ConnectionCommand::Echo { value }.into(),
        cli_command::Info { info } => server_command::ConnectionCommand::Info { info }.into(),
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
            server_command::DataCommand::Set {
                key,
                value,
                expire_at,
            }
            .into()
        }
        cli_command::Get { key } => server_command::DataCommand::Get { key }.into(),
        cli_command::Incr { key } => server_command::DataCommand::Increment { key }.into(),
        cli_command::Rpush { list_key, values } => {
            server_command::DataCommand::ListAppend { list_key, values }.into()
        }
        cli_command::Lpush { list_key, values } => {
            server_command::DataCommand::ListPrepend { list_key, values }.into()
        }
        cli_command::Lrange {
            list_key,
            start,
            end,
        } => server_command::DataCommand::ListRange {
            list_key,
            start,
            end,
        }
        .into(),
        cli_command::Llen { list_key } => server_command::DataCommand::ListLen { list_key }.into(),
        cli_command::Lpop {
            list_key,
            num_elems,
        } => server_command::DataCommand::ListPop {
            list_key,
            num_elems,
        }
        .into(),
        cli_command::Blpop { list_key, time_out } => {
            let timeout = if time_out == 0.0 {
                None
            } else {
                Some(Instant::now() + Duration::from_secs_f64(time_out))
            };
            server_command::DataCommand::BlockingListPop { list_key, timeout }.into()
        }
        cli_command::Type { key } => server_command::DataCommand::EntryType { key }.into(),
        cli_command::Xadd {
            key,
            stream_id,
            data,
        } => server_command::DataCommand::StreamAdd {
            key,
            stream_id,
            data,
        }
        .into(),
        cli_command::Xrange {
            key,
            lower_bound,
            upper_bound,
        } => server_command::DataCommand::StreamRange {
            key,
            lower_bound,
            upper_bound,
        }
        .into(),
        cli_command::Xread { args } => {
            // TODO: send proper error
            let XreadArgs {
                block,
                keys,
                streams,
            } = parse_xread_args(args.clone()).expect("expected valid args");
            match block {
                Some(block_millis) => server_command::DataCommand::BlockingStreamRead {
                    block_millis,
                    key: keys.into_iter().next().unwrap(),
                    stream: streams.into_iter().next().unwrap(),
                },
                None => server_command::DataCommand::StreamRead { keys, streams },
            }
            .into()
        }
        cli_command::Multi => server_command::DataCommand::StartTransaction.into(),
        cli_command::Exec => server_command::DataCommand::ExecuteTransaction.into(),
        cli_command::Discard => server_command::DataCommand::DiscardTransaction.into(),

        cli_command::Replconf { args } => {
            // args should have to values
            match args[0].to_lowercase().as_str() {
                "listening-port" => server_command::ReplicaCommand::ReplicateListeningPort,
                "capa" => server_command::ReplicaCommand::ReplicateCapabilities,
                "getack" => server_command::ReplicaCommand::ReplicateAcknowledge,
                _ => panic!("unknown replica command"),
            }
            .into()
        }
        cli_command::Psync { master_id, offset } => {
            server_command::ReplicaCommand::ReplicateSycn { master_id, offset }.into()
        }
    }
}
