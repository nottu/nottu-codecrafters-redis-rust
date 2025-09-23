use tokio::net::TcpListener;

use crate::server::Server;

use clap::Parser;

mod resp;
mod server;

#[derive(Debug, Parser)]
#[command(about = "A Redis server for CodeCrafters", version = "0.1")]
struct Cli {
    /// Port to listen on
    #[arg(short, long, default_value_t = 6379)]
    port: u16,

    #[arg(long)]
    replicaof: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let addr = format!("127.0.0.1:{}", cli.port);
    eprintln!("Listening on port {}", cli.port);

    let listener = TcpListener::bind(&addr).await?;

    let server = match cli.replicaof {
        None => Server::new(),
        Some(master) => {
            eprintln!("Replicating {master}");
            Server::replicate(master, &format!("{}", cli.port)).await?
        }
    };
    eprintln!("Server Booted {server:?}");
    loop {
        let (stream, addr) = listener.accept().await?;
        // Clone/Increase ref count out of spawn so it can be moved
        server.start_connection(stream, addr)?;
    }
}
