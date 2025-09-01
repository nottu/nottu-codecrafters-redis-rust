use std::{io::Write, net::TcpListener};

fn main() -> anyhow::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379")?;

    for stream in listener.incoming() {
        let mut stream = stream?;
        stream.write_all(b"+PONG\r\n")?;
    }
    Ok(())
}
