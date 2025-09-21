use std::{fmt, io::Cursor};

use anyhow::bail;
use bytes::Buf;
use clap::Parser;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufWriter},
    net::TcpStream,
};

use crate::{
    cli_commands::{Command, RedisCli},
    resp::Frame,
};

pub struct Connection {
    stream: BufWriter<TcpStream>,
    buf: [u8; Self::BUF_SIZE],
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
    const BUF_SIZE: usize = 1024;
    pub fn new(stream: TcpStream) -> Self {
        Self {
            stream: BufWriter::new(stream),
            buf: [0; Self::BUF_SIZE],
        }
    }
    pub async fn read_command(&mut self) -> anyhow::Result<Command> {
        let data = self.read_frame().await?;
        let Frame::Array(command_args) = data else {
            bail!("expected an array with command and arguments, got {data:?}")
        };
        let parsed_commands = RedisCli::try_parse_from(
            // clap wants the first arg to be the program name... we pre-pend a value to comply
            std::iter::once(Frame::SimpleString("redis-cli".to_string())).chain(command_args),
        )?;
        Ok(parsed_commands.command)
    }
    pub async fn read_frame(&mut self) -> anyhow::Result<Frame> {
        let bytes_read = self.stream.read(&mut self.buf).await?;
        if bytes_read == 0 {
            bail!("Connection closed")
        }

        let mut cursor = Cursor::new(&self.buf[..bytes_read]);
        let frame = Frame::try_parse(&mut cursor)?;
        if cursor.has_remaining() {
            eprintln!("Remainig bytes in cursor!");
        }
        Ok(frame)
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
        // self.stream.write_all(&frame.to_string().as_bytes()).await?;
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
