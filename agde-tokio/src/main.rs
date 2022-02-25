use std::collections::HashMap;
use std::error::Error;
use std::fmt::{self, Display};
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use std::{io, process};

use agde::Manager;
use futures::{Future, FutureExt, SinkExt, StreamExt, TryFutureExt};
use log::{error, info, warn};
use serde::{Deserialize, Serialize};
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

#[derive(Serialize, Deserialize)]
struct Metadata {
    /// Associate resource to metadata
    map: HashMap<String, ResourceMeta>,
}
impl Metadata {
    async fn new() -> Result<Self, io::Error> {
        let map: Result<HashMap<String, ResourceMeta>, io::Error> =
            tokio::task::spawn_blocking(|| {
                let mut map = HashMap::new();

                for entry in walkdir::WalkDir::new(".")
                    .follow_links(false)
                    .into_iter()
                    .filter_map(|e| e.ok())
                {
                    if let Some(path) = entry.path().to_str() {
                        let metadata = entry.metadata()?;
                        let modified = metadata.modified()?;

                        map.insert(
                            path.to_owned(),
                            ResourceMeta {
                                size: metadata.len(),
                                actual_mtime: modified,
                                public_mtime: modified,
                            },
                        );
                    } else {
                        error!(
                            "Directory contains non-UTF8 filename. Skipping {:?}",
                            entry.path().to_string_lossy()
                        );
                    }
                }

                Ok(map)
            })
            .await
            .expect("walkdir thread panicked");

        let map = map?;

        Ok(Self { map })
    }
    async fn sync(&self) -> Result<(), io::Error> {
        let data = bincode::serde::encode_to_vec(
            self,
            bincode::config::standard().skip_fixed_array_length(),
        )
        .expect("serialization of metadata should be infallible");
        tokio::fs::write(".agde/metadata", data).await?;
        Ok(())
    }
}
#[derive(Serialize, Deserialize)]
struct ResourceMeta {
    /// the mtime on the disk might be different from what is expected.
    actual_mtime: SystemTime,
    public_mtime: SystemTime,
    size: u64,
}
enum Change {
    Create(String),
    Modify(String),
    Delete(String),
}

pub type BoxFut<T> = Pin<Box<dyn Future<Output = T> + Send>>;
pub type ReadFuture = BoxFut<Result<Vec<u8>, ()>>;
pub type WriteFuture = BoxFut<Result<(), ()>>;
pub type DiffFuture = BoxFut<Result<Vec<Change>, ()>>;
pub type ReadFn = Box<dyn Fn(String) -> ReadFuture + Send + Sync>;
pub type WriteFn = Box<dyn Fn(String, Vec<u8>) -> WriteFuture + Send + Sync>;
pub type DeleteFn = Box<dyn Fn(String) -> WriteFuture + Send + Sync>;
pub type DiffFn = Box<dyn Fn() -> DiffFuture + Send + Sync>;
#[must_use]
pub struct Options {
    pub read: ReadFn,
    pub write: WriteFn,
    pub delete: DeleteFn,
    /// Returns a list of the resources which might have changed.
    pub rough_resource_diff: DiffFn,

    /// For how long to wait for welcomes.
    pub startup_timeout: Duration,
    pub sync_interval: Duration,
}
// `TODO`: (see next paragraph) How to solve storages? These methods should be writing to the public storage, but how do
// we sync that to the current? Just check if the current hasn't changed (write to current too),
// but if it has, wait till next commit from current to public.
// Have enum to the functions below, which tells them on which to operate?
//
// You can also say to it to always wait for the next commit. (**this is what we implement
// initially**).
impl Options {
    pub async fn fs() -> Result<Self, io::Error> {
        let metadata = initial_metadata().await?;
        let metadata = Arc::new(Mutex::new(metadata));
        Ok(Options {
            read: Box::new(|resource| {
                Box::pin(async move {
                    let mut file = tokio::fs::File::open(&resource).await.map_err(|_| ())?;
                    let mut buf = Vec::with_capacity(4096);
                    file.read_to_end(&mut buf).await.map_err(|_| ())?;
                    Ok(buf)
                })
            }),
            // `TODO`: When writing to public storage, also set metadata.
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
            rough_resource_diff: Box::new(move || {
                let metadata = Arc::clone(&metadata);
                Box::pin(async move {
                    let mut changed = Vec::new();
                    let metadata = metadata.lock().await;
                    let current_metadata = Metadata::new().await.map_err(|_| ())?;
                    for (resource, meta) in &metadata.map {
                        match current_metadata.map.get(resource) {
                            Some(data) => {
                                if data.actual_mtime != meta.actual_mtime || data.size != meta.size
                                {
                                    changed.push(Change::Modify(resource.clone()));
                                }
                            }
                            None => changed.push(Change::Delete(resource.clone())),
                        }
                    }
                    for resource in current_metadata.map.keys() {
                        if !metadata.map.contains_key(resource) {
                            changed.push(Change::Create(resource.clone()))
                        }
                    }
                    Ok(changed)
                })
            }),
            startup_timeout: Duration::from_secs(7),
            sync_interval: Duration::from_secs(5),
        })
    }
    pub fn arc(self) -> Arc<Self> {
        Arc::new(self)
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
        let options = Options::fs()
            .await
            .expect("failed to read file system metadata")
            .arc();

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

async fn run(url: &str, mut manager: Manager, options: Arc<Options>) -> Result<(), DynError> {
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

        info!(
            "Sent hello. Waiting for {}s for piers to welcome.",
            options.startup_timeout.as_secs_f64()
        );

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
                                    agde::MessageKind::Hello(hello) => {
                                        // copied from loop below.
                                        info!("Pier {} joined the network.", hello.uuid());
                                        let msg = manager.apply_hello(hello);
                                        if write.send(msg.to_bin().into()).await.is_err() {
                                            return Err(Box::new(ApplicationError::StreamBroken));
                                        }
                                    }
                                    agde::MessageKind::Welcome { info, recipient: _ } => {
                                        info!("Pier {} welcomes you.", info.uuid());
                                        manager.apply_welcome(info.clone());
                                    }
                                    agde::MessageKind::InvalidUuid(sender) => {
                                        info!("Pier {} claims your UUID isn't unique.", sender);
                                        rejections += 1;
                                    }
                                    agde::MessageKind::MismatchingVersions(sender) => {
                                        warn!(
                                            "Pier {} claims you have a mismatching version.",
                                            sender
                                        );
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

        // if 1/2 of total is rejections
        let try_again = rejections * 2 > total;

        if try_again {
            return Err(Box::new(ApplicationError::PiersRejected));
        } else {
            // continue normally.
        }
    }

    let write = Arc::new(Mutex::new(write));
    let read = Arc::new(Mutex::new(read));
    let manager = Arc::new(Mutex::new(manager));

    let accept_handle = {
        let manager = Arc::clone(&manager);
        let write = Arc::clone(&write);
        let options = Arc::clone(&options);

        // event handler
        tokio::spawn(async move {
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
                            warn!(
                                "Received invalid binary message. A pier might be faulty. Data: {}",
                                String::from_utf8_lossy(&data)
                            );
                            continue;
                        };

                        let mut manager = manager.lock().await;
                        match message.inner() {
                            agde::MessageKind::Hello(hello) => {
                                info!("Pier {} joined the network.", hello.uuid());
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
                                                    (options.read)(resource.to_owned())
                                                        .await
                                                        .map_err(|()| {
                                                            ApplicationError::StoragePermissions
                                                        })?;
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
                                                .map_err(|()| {
                                                    ApplicationError::StoragePermissions
                                                })?;
                                        }
                                        agde::EventKind::Delete(ev) => {
                                            (options.delete)(ev.resource().to_owned())
                                                .await
                                                .map_err(|()| {
                                                    ApplicationError::StoragePermissions
                                                })?;
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
                            agde::MessageKind::EventUuidLogCheck { uuid: _, check: _ } => todo!(),
                            agde::MessageKind::EventUuidLogCheckReply { uuid: _, check: _ } => {
                                todo!()
                            }
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
            Err(ApplicationError::UnexpectedServerClose)
        })
    };

    info!("Began listening to messages.");

    let local_watcher: tokio::task::JoinHandle<Result<(), ApplicationError>> = {
        let options = Arc::clone(&options);
        tokio::spawn(async move {
            loop {
                let diff = (options.rough_resource_diff)()
                    .await
                    .map_err(|_| ApplicationError::StoragePermissions)?;
                let mut events = Vec::with_capacity(diff.len());
                {
                    let mut manager = manager.lock().await;

                    for diff in diff {
                        let message = match diff {
                            Change::Create(res) => {
                                manager.process_event(agde::event::Create::new(res))
                            }
                            // `TODO`: Give successor
                            Change::Delete(res) => {
                                manager.process_event(agde::event::Delete::new(res, None))
                            }
                            Change::Modify(res) => {
                                let data = (options.read)(res.clone())
                                    .await
                                    .map_err(|_| ApplicationError::StoragePermissions)?;
                                let len = data.len();
                                // `TODO`: read from public
                                let base = (options.read)(res.clone())
                                    .await
                                    .map_err(|_| ApplicationError::StoragePermissions)?;

                                let event = agde::event::Modify::new(
                                    res,
                                    vec![agde::VecSection::whole_resource(len, data)],
                                    Some(&base),
                                );
                                manager.process_event(event)
                            }
                        };

                        events.push(message)
                    }
                }
                // `TODO`: Send events, and apply them to our setup.
                // IMPORTANT: It's also here we have to take into account the data other have produced in the
                // meantime. Maybe before creating messages, take our diffs, unwind as if we edited
                // at the time of the last storage sync, then see where they landed now.

                tokio::time::sleep(options.sync_interval).await;
            }
        })
    };

    let result = match futures::future::select(accept_handle, local_watcher).await {
        futures::future::Either::Left((result, other))
        | futures::future::Either::Right((result, other)) => {
            other.abort();
            result.expect("task panicked")
        }
    };
    result.map_err(|err| Box::new(err) as DynError)
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
async fn initial_metadata() -> Result<Metadata, io::Error> {
    tokio::fs::create_dir_all(".agde").await?;
    let metadata = tokio::fs::read(".agde/metadata")
        .then(|r| async move { r.map_err(|_| ()) })
        .and_then(|data| async move {
            bincode::serde::decode_from_slice(
                &data,
                bincode::config::standard().skip_fixed_array_length(),
            )
            .map_err(|_| ())
        });
    match metadata.await {
        Ok((metadata, _)) => Ok(metadata),
        Err(_) => {
            error!("Metadata corrupt. Recreating.");

            let metadata = Metadata::new().await?;
            if let Err(err) = metadata.sync().await {
                error!("Failed to write newly created metadata: {err:?}");
            }
            Ok(metadata)
        }
    }
}
