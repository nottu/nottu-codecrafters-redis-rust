use anyhow::bail;
use clap::Parser;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufWriter},
    net::TcpStream,
};

use crate::{
    commands::{Command, RedisCli},
    resp::Frame,
};

pub struct Connection {
    stream: BufWriter<TcpStream>,
    buf: [u8; Self::BUF_SIZE],
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
        let bytes_read = self.stream.read(&mut self.buf).await?;
        if bytes_read == 0 {
            bail!("Connection closed")
        }

        let data = str::from_utf8(&self.buf[..bytes_read])?;
        dbg!(data);
        let data = Frame::try_parse(data)?;
        let Frame::Array(command_args) = data else {
            bail!("expected an array with command and arguments, got {data:?}")
        };
        let parsed_commands = RedisCli::try_parse_from(
            // clap wants the first arg to be the program name... we pre-pend a value to comply
            std::iter::once(Frame::SimpleString("redis-cli".to_string())).chain(command_args),
        )?;
        Ok(parsed_commands.command)
    }
    pub async fn write_nil(&mut self) -> anyhow::Result<()> {
        self.stream.write_all(b"$-1\r\n").await?;
        Ok(())
    }
    pub async fn write_nil_array(&mut self) -> anyhow::Result<()> {
        self.stream.write_all(b"*-1\r\n").await?;
        Ok(())
    }
    pub async fn write_simple(&mut self, s: &str) -> anyhow::Result<()> {
        self.stream.write(b"+").await?;
        self.stream.write(s.as_bytes()).await?;
        self.stream.write(b"\r\n").await?;
        self.stream.flush().await?;
        Ok(())
    }
    pub async fn write_bytes(&mut self, data: &[u8]) -> anyhow::Result<()> {
        self.stream.write_all(b"$").await?;
        self.write_decimal(data.len() as u64).await?;
        self.stream.write(data).await?;
        self.stream.write(b"\r\n").await?;
        Ok(())
    }
    async fn write_decimal(&mut self, val: u64) -> anyhow::Result<()> {
        use std::io::{Cursor, Write};

        let buf = [0u8; 20];
        let mut buf = Cursor::new(buf);
        write!(&mut buf, "{}", val)?;
        let pos = buf.position() as usize;

        self.stream.write_all(&buf.get_ref()[..pos]).await?;
        self.stream.write_all(b"\r\n").await?;
        Ok(())
    }
    pub async fn write_int(&mut self, val: u64) -> anyhow::Result<()> {
        self.stream.write_all(b":").await?;
        self.write_decimal(val).await?;
        Ok(())
    }
    pub async fn write_array(&mut self, data: &[Vec<u8>]) -> anyhow::Result<()> {
        self.stream.write_all(b"*").await?;
        self.write_decimal(data.len() as u64).await?;
        for d in data {
            self.write_bytes(d).await?;
        }
        Ok(())
    }
    pub async fn write_simple_err(&mut self, err: &str) -> anyhow::Result<()> {
        self.stream.write_all(b"-").await?;
        self.stream.write_all(err.as_bytes()).await?;
        self.stream.write_all(b"\r\n").await?;
        Ok(())
    }
    pub async fn write_frame(&mut self, frame: Frame) -> anyhow::Result<()> {
        // TODO avoid creating a new String...
        self.stream.write_all(&frame.to_string().as_bytes()).await?;
        Ok(())
    }
    pub async fn flush(&mut self) -> anyhow::Result<()> {
        self.stream.flush().await?;
        Ok(())
    }
}
