use std::error::Error;
use std::process;
use std::sync::Arc;
use std::time::Duration;

use agde::Manager;
use futures::{SinkExt, StreamExt};
use log::{error, info};
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite;

type DynError = Box<dyn Error>;
type WsStream =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;
type WriteHalf = futures::stream::SplitSink<WsStream, tungstenite::Message>;
type ReadHalf = futures::stream::SplitStream<WsStream>;

#[tokio::main]
async fn main() {
    env_logger::init();

    // `TODO`: use clap for argument parsing.
    let url = "ws://localhost:8081/ws";

    loop {
        let manager = make_manager();

        match run(url, manager).await {
            Ok(()) => process::exit(0),
            Err(err) => {
                error!("Got error: {err}. Trying to reconnect in 10s.");
                tokio::time::sleep(Duration::from_secs(10)).await;
            }
        }
    }
}

async fn run(url: &str, manager: Manager) -> Result<(), DynError> {
    let (write, read) = connect_ws(url).await?;
    let write = Arc::new(Mutex::new(write));
    let read = Arc::new(Mutex::new(read));

    let manager = Arc::new(Mutex::new(manager));

    {
        let mut write = write.lock().await;
        write.send(tungstenite::Message::text("HI!")).await?;
    }
    {
        let mut read = read.lock().await;
        let message = read.next().await.unwrap()?;
        println!("Recieved {:?}", message);
    }
    let mgr = manager.clone();
    // event handler
    tokio::spawn(async move {
        let manager = mgr;
        let read = read.lock().await;

        while let Some(message) = read.next().await {
            let message = match message {
                Ok(m) => m,
                Err(err) => {
                    error!("Got error from WebSocket: {err}");
                    continue;
                }
            };
        }
    });
    // Ok(())
    Err(Box::new("unexpected close from server") as DynError)
}
async fn connect_ws(url: &str) -> Result<(WriteHalf, ReadHalf), DynError> {
    info!("Connecting to {url:?}.");
    let result = tokio_tungstenite::connect_async(url).await;
    let conenction = result?;
    Ok(conenction.0.split())
}
fn make_manager() -> Manager {
    Manager::new(false, 0, Duration::from_secs(60), 512)
}
