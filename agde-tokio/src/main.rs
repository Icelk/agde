use std::collections::HashMap;
use std::error::Error;
use std::fmt::{self, Display};
use std::path::Path;
use std::pin::Pin;
use std::process;
use std::sync::Arc;
use std::time::Duration;

use agde::Manager;
use futures::{Future, SinkExt, StreamExt};
use log::{error, info, warn};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite;

type DynError = Box<dyn Error>;
type WsStream =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;
type WriteHalf = futures::stream::SplitSink<WsStream, tungstenite::Message>;
type ReadHalf = futures::stream::SplitStream<WsStream>;

#[derive(Debug, PartialEq)]
pub enum ApplicationError {
    UnexpectedServerClose,
    StoragePermissions,
    StreamBroken,
    PiersRejected,
}
impl Error for ApplicationError {}
impl Display for ApplicationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnexpectedServerClose => write!(f, "unexpected server close"),
            Self::StoragePermissions => write!(f, "insufficient permissions for local storage"),
            Self::StreamBroken => write!(f, "stream to server unexpectedly closed"),
            Self::PiersRejected => write!(
                f,
                "the other clients rejected you because of invalid UUID / version"
            ),
        }
    }
}

pub type BoxFut<T> = Pin<Box<dyn Future<Output = T> + Send>>;
pub type ReadFuture = BoxFut<Result<Vec<u8>, ()>>;
pub type ReadFn = Box<dyn Fn(String) -> ReadFuture + Send + Sync>;
pub type WriteFuture = BoxFut<Result<(), ()>>;
pub type WriteFn = Box<dyn Fn(String, Vec<u8>) -> WriteFuture + Send + Sync>;
pub type DeleteFn = Box<dyn Fn(String) -> WriteFuture + Send + Sync>;
pub struct Options {
    pub read: ReadFn,
    pub write: WriteFn,
    pub delete: DeleteFn,

    /// For how long to wait for welcomes.
    pub startup_timeout: Duration,
}
impl Options {
    pub fn fs() -> Self {
        Options {
            read: Box::new(|resource| {
                Box::pin(async move {
                    let mut file = tokio::fs::File::open(&resource).await.map_err(|_| ())?;
                    let mut buf = Vec::with_capacity(4096);
                    file.read_to_end(&mut buf).await.map_err(|_| ())?;
                    Ok(buf)
                })
            }),
            write: Box::new(|resource, data| {
                Box::pin(async move {
                    let mut file = tokio::fs::File::create(&resource).await.map_err(|_| ())?;
                    file.write_all(&data).await.map_err(|_| ())?;
                    file.flush().await.map_err(|_| ())?;
                    Ok(())
                })
            }),
            delete: Box::new(|resource| {
                Box::pin(async move {
                    let path = Path::new(&resource);
                    if tokio::fs::metadata(path).await.map_err(|_| ())?.is_file() {
                        tokio::fs::remove_file(path).await.map_err(|_| ())?;
                    } else {
                        tokio::fs::remove_dir_all(path).await.map_err(|_| ())?;
                    }
                    Ok(())
                })
            }),
            startup_timeout: Duration::from_secs(7),
        }
    }
    pub fn with_startup_duration(mut self, startup_timeout: Duration) -> Self {
        self.startup_timeout = startup_timeout;
        self
    }
}

#[tokio::main]
async fn main() {
    env_logger::init();

    // `TODO`: use clap for argument parsing.
    let url = "ws://localhost:8081/ws";

    loop {
        let options = Options::fs();

        let manager = make_manager();

        match run(url, manager, options).await {
            Ok(()) => process::exit(0),
            Err(err) => {
                error!("Got error: {err}. Trying to reconnect in 10s.");
                tokio::time::sleep(Duration::from_secs(10)).await;
            }
        }
    }
}

async fn run(url: &str, mut manager: Manager, options: Options) -> Result<(), DynError> {
    let (mut write, mut read) = connect_ws(url).await?;

    {
        write.send(tungstenite::Message::text("HI!")).await?;
    }
    {
        let message = read.next().await.unwrap()?;
        println!("Recieved {:?}", message);
    }
    {
        let message = { manager.process_hello() };
        write
            .send(message.to_bin().into())
            .await
            .map_err(|_| ApplicationError::UnexpectedServerClose)?;

        let mut total = 0;
        let mut rejections = 0;

        loop {
            let sleep = Box::pin(tokio::time::sleep(options.startup_timeout));
            match futures::future::select(sleep, read.next()).await {
                // Sleep timeout
                futures::future::Either::Left(((), _)) => {
                    break;
                }
                // message
                futures::future::Either::Right((message, _)) => {
                    let message = if let Some(Ok(m)) = message {
                        m
                    } else {
                        return Err(Box::new(ApplicationError::UnexpectedServerClose) as DynError);
                    };

                    match message {
                        tungstenite::Message::Text(text) => {
                            warn!("Recieved text from server: {text:?}");
                        }
                        tungstenite::Message::Binary(data) => {
                            if let Ok(message) = agde::Message::from_bin(&data) {
                                total += 1;
                                match message.inner() {
                                    agde::MessageKind::Welcome { info, recipient: _ } => {
                                        manager.apply_welcome(info.clone());
                                    }
                                    agde::MessageKind::InvalidUuid(_sender) => {
                                        rejections += 1;
                                    }
                                    agde::MessageKind::MismatchingVersions(_sender) => {
                                        warn!("Pier claims you have a mismatching version.");
                                        rejections += 1;
                                    }
                                    _ => {}
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }
        }

        // if total > 2 and if 2/3 of total is rejections
        let try_again = total > 2 && rejections * 3 > total * 2;

        if try_again {
            return Err(Box::new(ApplicationError::PiersRejected));
        } else {
            // continue normally.
        }
    }

    let write = Arc::new(Mutex::new(write));
    let read = Arc::new(Mutex::new(read));
    let manager = Arc::new(Mutex::new(manager));

    let mgr = manager.clone();
    let write_handle = write.clone();
    // event handler
    let result = tokio::spawn(async move {
        let manager = mgr;
        let write = write_handle;
        let mut read = read.lock().await;

        while let Some(message) = read.next().await {
            let message = match message {
                Ok(m) => m,
                Err(err) => {
                    error!("Got error from WebSocket: {err}");
                    continue;
                }
            };
            match message {
                tungstenite::Message::Text(text) => {
                    warn!("Recieved text from server: {text:?}");
                }
                tungstenite::Message::Binary(data) => {
                    let message = agde::Message::from_bin(&data);
                    let message = if let Ok(m) = message {
                        m
                    } else {
                        warn!("Received invalid binary message. A pier might be faulty.");
                        continue;
                    };

                    let mut manager = manager.lock().await;
                    match message.inner() {
                        agde::MessageKind::Hello(hello) => {
                            let msg = manager.apply_hello(hello);
                            let mut w = write.lock().await;
                            if w.send(msg.to_bin().into()).await.is_err() {
                                return Err(ApplicationError::StreamBroken);
                            }
                        }
                        agde::MessageKind::Welcome { info, recipient } => {
                            if recipient.map_or(true, |intended| intended == manager.uuid()) {
                                manager.apply_welcome(info.clone());
                            }
                        }
                        // ignore initial messages once connected.
                        agde::MessageKind::InvalidUuid(_)
                        | agde::MessageKind::MismatchingVersions(_) => {}
                        agde::MessageKind::Event(event) => {
                            match manager.apply_event(event, message.uuid()) {
                                Ok(applier) => match event.inner() {
                                    agde::EventKind::Modify(ev) => {
                                        if let Some(resource) = applier.resource() {
                                            let mut resource_data =
                                                (options.read)(resource.to_owned()).await.map_err(
                                                    |()| ApplicationError::StoragePermissions,
                                                )?;
                                            let len = resource_data.len();
                                            let mut slice = agde::SliceBuf::with_filled(
                                                &mut resource_data,
                                                len,
                                            );
                                            slice.extend_to_needed(ev.sections(), 0);
                                            applier.apply(&mut slice).unwrap();
                                        } else {
                                            // do nothing, as the doc says
                                        }
                                    }
                                    agde::EventKind::Create(ev) => {
                                        (options.write)(ev.resource().to_owned(), Vec::new())
                                            .await
                                            .map_err(|()| ApplicationError::StoragePermissions)?;
                                    }
                                    agde::EventKind::Delete(ev) => {
                                        (options.delete)(ev.resource().to_owned())
                                            .await
                                            .map_err(|()| ApplicationError::StoragePermissions)?;
                                    }
                                },
                                Err(err) => {
                                    warn!("Slow pier. Got error from internal log: {err:?}. Running a log check.");
                                    // `TODO`: Log check!
                                }
                            };
                        }
                        agde::MessageKind::FastForward => todo!(),
                        agde::MessageKind::FastForwardReply => todo!(),
                        agde::MessageKind::Sync(_) => todo!(),
                        agde::MessageKind::SyncReply(_) => todo!(),
                        agde::MessageKind::HashCheck(_) => todo!(),
                        agde::MessageKind::HashCheckReply(_) => todo!(),
                        agde::MessageKind::EventUuidLogCheck { uuid, check } => todo!(),
                        agde::MessageKind::EventUuidLogCheckReply { uuid, check } => todo!(),
                        // Have a map of current conversations with piers.
                        // If a Cancelled received, look up if we have a conversation (Enum of kind, list of tried piers), and call
                        // the appropriate function to initiate a new one, ignoring all the tried.
                        // If we get no result, give up.
                        agde::MessageKind::Cancelled(_) => todo!(),
                    }
                }
                tungstenite::Message::Close(_) => {
                    return Err(ApplicationError::UnexpectedServerClose);
                }
                // ping/pong frames
                // raw frame isn't possible when receiving, see https://docs.rs/tungstenite/0.17.2/tungstenite/enum.Message.html#variant.Frame
                _ => {}
            }
        }
        Ok(())
    });

    result
        .await
        .expect("receiving task panicked")
        .map_err(|err| Box::new(err) as DynError)
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
