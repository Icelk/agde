use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt::{self, Display};
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use std::{io, process};

use agde::Manager;
use futures::{Future, FutureExt, SinkExt, StreamExt, TryFutureExt};
use log::{debug, error, info, warn};
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

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Metadata {
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
                    .filter_entry(|e| {
                        !(e.path().starts_with(".agde")
                            || e.path().starts_with("./.agde")
                            || e.path().starts_with("./node_modules"))
                    })
                    .filter_map(|e| e.ok())
                {
                    if let Some(path) = entry.path().to_str() {
                        let metadata = entry.metadata()?;
                        if !metadata.is_file() {
                            continue;
                        }
                        let modified = metadata.modified()?;

                        map.insert(
                            path.to_owned(),
                            ResourceMeta {
                                size: metadata.len(),
                                current_mtime: Some(modified),
                                // public_mtime: modified,
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
            bincode::config::standard().write_fixed_array_length(),
        )
        .expect("serialization of metadata should be infallible");
        tokio::fs::write(".agde/metadata", data).await?;
        Ok(())
    }
}
#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub struct ResourceMeta {
    /// mtime of the [`Storage::Current`].
    ///
    /// Is [`None`] if this resource has not yet been written to the current storage.
    /// In that case, there's always a change. This can occur when data has been written to the
    /// public storage, and the current storage hasn't gotten that data yet.
    pub current_mtime: Option<SystemTime>,
    // `TODO`: remove this, since we're not using it.
    // pub public_mtime: SystemTime,
    pub size: u64,
}
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Change {
    /// True if the resource was just created.
    Modify(String, bool),
    Delete(String),
}
impl Change {
    pub fn resource(&self) -> &str {
        match self {
            Self::Modify(r, _) => r,
            Self::Delete(r) => r,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Storage {
    /// The copy of data which is maintained to be equal to the others' public storages.
    Public,
    /// The copy of data the user writes to.
    Current,
}
pub enum WriteMtime {
    LookUpCurrent,
    No,
}

pub type BoxFut<T> = Pin<Box<dyn Future<Output = T> + Send>>;
pub type ReadFuture = BoxFut<Result<Option<Vec<u8>>, ()>>;
pub type WriteFuture = BoxFut<Result<(), ()>>;
pub type DiffFuture = BoxFut<Result<Vec<Change>, ()>>;
pub type ReadFn = Box<dyn Fn(String, Storage) -> ReadFuture + Send + Sync>;
/// The [`SystemTime`] is the time of modification.
/// When [`Storage::Current`], this should be the time of last change.
pub type WriteFn = Box<
    dyn Fn(String, Storage, Vec<u8>, WriteMtime /*, ResourceMeta*/) -> WriteFuture + Send + Sync,
>;
pub type DeleteFn = Box<dyn Fn(String, Storage) -> WriteFuture + Send + Sync>;
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
// `TODO`: Add option to write new resource changes to `Current` is that resource hasn't been
// changed in current.
impl Options {
    pub async fn fs() -> Result<Self, io::Error> {
        let metadata = initial_metadata().await?;
        // A metadata cache that is held constant between diff calls.
        let offline_metadata = metadata.clone();

        let metadata = Arc::new(Mutex::new(metadata));
        let offline_metadata = Arc::new(Mutex::new(offline_metadata));
        let write_metadata = Arc::clone(&metadata);
        let write_offline_metadata = Arc::clone(&offline_metadata);
        let delete_metadata = Arc::clone(&metadata);
        let delete_offline_metadata = Arc::clone(&offline_metadata);
        Ok(Options {
            read: Box::new(|resource, storage| {
                Box::pin(async move {
                    let path = match storage {
                        Storage::Public => format!(".agde/{resource}",),
                        Storage::Current => format!("./{resource}"),
                    };
                    let file = tokio::fs::File::open(&path).await;
                    let mut file = match file {
                        Ok(f) => f,
                        Err(err) => match err.kind() {
                            io::ErrorKind::NotFound => return Ok(None),
                            _ => return Err(()),
                        },
                    };
                    let mut buf = Vec::with_capacity(4096);
                    file.read_to_end(&mut buf).await.map_err(|_| ())?;
                    Ok(Some(buf))
                })
            }),
            write: Box::new(move |resource, storage, data, mtime /*, metadata*/| {
                let metadata = Arc::clone(&write_metadata);
                let offline_metadata = Arc::clone(&write_offline_metadata);
                Box::pin(async move {
                    let path = match storage {
                        Storage::Public => format!(".agde/{resource}",),
                        Storage::Current => format!("./{resource}"),
                    };
                    let mut file = tokio::fs::File::create(&path).await.map_err(|_| ())?;
                    file.write_all(&data).await.map_err(|_| ())?;
                    file.flush().await.map_err(|_| ())?;
                    match storage {
                        Storage::Public => {
                            let mut metadata = metadata.lock().await;
                            // metadata
                            // .map
                            // .entry(resource)
                            // .or_insert_with(|| ResourceMeta {
                            // current_mtime: None,
                            // public_mtime: mtime,
                            // size: data.len() as u64,
                            // });
                            // let mtime = metadata
                            // .map
                            // .get(&resource)
                            // .and_then(|meta| meta.current_mtime);
                            let mtime = match mtime {
                                WriteMtime::No => None,
                                WriteMtime::LookUpCurrent => {
                                    let metadata = tokio::fs::metadata(format!("./{resource}"))
                                        .await
                                        .map_err(|_| ())?;
                                    let mtime = metadata.modified().map_err(|_| ())?;
                                    Some(mtime)
                                }
                            };
                            let meta = ResourceMeta {
                                current_mtime: mtime,
                                // public_mtime: mtime,
                                size: data.len() as u64,
                            };
                            metadata.map.insert(resource, meta);
                        }
                        Storage::Current => {
                            let file_metadata = tokio::fs::metadata(&path).await.map_err(|_| ())?;
                            let mtime = file_metadata.modified().map_err(|_| ())?;
                            {
                                let mut metadata = offline_metadata.lock().await;

                                metadata.map.insert(
                                    resource.clone(),
                                    ResourceMeta {
                                        current_mtime: Some(mtime),
                                        size: data.len() as u64,
                                    },
                                );
                            }
                            {
                                let mut metadata = metadata.lock().await;

                                metadata.map.insert(
                                    resource,
                                    ResourceMeta {
                                        current_mtime: Some(mtime),
                                        size: data.len() as u64,
                                    },
                                );
                                metadata.sync().await.map_err(|_| ())?;
                            }
                        }
                    }
                    Ok(())
                })
            }),
            delete: Box::new(move |resource, storage| {
                let metadata = Arc::clone(&delete_metadata);
                let offline_metadata = Arc::clone(&delete_offline_metadata);
                Box::pin(async move {
                    let path = match storage {
                        Storage::Public => format!(".agde/{resource}",),
                        Storage::Current => format!("./{resource}"),
                    };
                    if storage == Storage::Current {
                        let mut metadata = offline_metadata.lock().await;
                        debug!("Removing {resource} from metdata cache.");
                        metadata.map.remove(&resource);
                    }
                    {
                        let mut metadata = metadata.lock().await;
                        debug!("Removing {resource} from metdata cache.");
                        metadata.map.remove(&resource);
                        metadata.sync().await.map_err(|_| ())?;
                    }
                    let file_metadata = match tokio::fs::metadata(&path).await {
                        Ok(d) => d,
                        Err(err) => match err.kind() {
                            io::ErrorKind::NotFound => return Ok(()),
                            _ => return Err(()),
                        },
                    };
                    if file_metadata.is_file() {
                        tokio::fs::remove_file(&path).await.map_err(|_| ())?;
                    } else {
                        tokio::fs::remove_dir_all(&path).await.map_err(|_| ())?;
                    }

                    Ok(())
                })
            }),
            rough_resource_diff: Box::new(move || {
                let metadata = Arc::clone(&metadata);
                let offline_metadata = Arc::clone(&offline_metadata);
                Box::pin(async move {
                    debug!("Getting diff");
                    info!("metadata: {:?}", &*metadata.lock().await);
                    let mut changed = Vec::new();
                    let mut offline_metadata = offline_metadata.lock().await;
                    let current_metadata = Metadata::new().await.map_err(|_| ())?;
                    for (resource, meta) in &offline_metadata.map {
                        match current_metadata.map.get(resource) {
                            Some(current_data) => {
                                info!(
                                    "Comparing mtime {:?} {:?}",
                                    meta.current_mtime, current_data.current_mtime
                                );
                                if meta.current_mtime.map_or(true, |mtime| {
                                    mtime
                                        != current_data
                                            .current_mtime
                                            .expect("we just created this from local metadata")
                                }) || current_data.size != meta.size
                                {
                                    info!(
                                        "Claiming {:?} != {:?} || {} != {}",
                                        meta.current_mtime,
                                        current_data.current_mtime,
                                        current_data.size,
                                        meta.size
                                    );
                                    changed.push(Change::Modify(resource.clone(), false));
                                }
                            }
                            None => changed.push(Change::Delete(resource.clone())),
                        }
                    }
                    for resource in current_metadata.map.keys() {
                        if !offline_metadata.map.contains_key(resource) {
                            changed.push(Change::Modify(resource.clone(), true))
                        }
                    }
                    let mut metadata = metadata.lock().await;
                    for change in &changed {
                        match change {
                            Change::Modify(res, _) => {
                                warn!("Insert {res} into metadata");
                                metadata
                                    .map
                                    .insert(res.clone(), *current_metadata.map.get(res).unwrap());
                            }
                            Change::Delete(res) => {
                                metadata.map.remove(res);
                            }
                        }
                    }
                    {
                        // let metadata = metadata.lock().await;
                        // `TODO`: Optimize this
                        *offline_metadata = metadata.clone();
                        metadata.sync().await.map_err(|_| ())?;
                    }
                    debug!("Changed: {:?}", changed);
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
        let mut options = Options::fs()
            .await
            .expect("failed to read file system metadata");
        options.startup_timeout = Duration::from_secs(1);
        let options = options.arc();

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

    // changes since last Storage sync
    let changed = Arc::new(Mutex::new(HashSet::new()));

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
        let changed = Arc::clone(&changed);

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
                                info!("Got event from pier {}: {event:?}", message.sender());
                                {
                                    changed.lock().await.insert(event.resource().to_owned());
                                }

                                match manager.apply_event(event, message.uuid()) {
                                    Ok(applier) => match event.inner() {
                                        agde::EventKind::Modify(ev) => {
                                            if let Some(resource) = applier.resource() {
                                                let resource_data = (options.read)(
                                                    resource.to_owned(),
                                                    Storage::Public,
                                                )
                                                .await
                                                .map_err(|()| {
                                                    ApplicationError::StoragePermissions
                                                })?;
                                                if let Some(mut data) = resource_data {
                                                    let mut slice =
                                                        agde::SliceBuf::with_whole(&mut data);
                                                    slice.extend_to_needed(ev.sections(), 0);
                                                    applier.apply(&mut slice).unwrap();
                                                    let len = slice.filled().len();
                                                    data.truncate(len);
                                                    (options.write)(
                                                        resource.to_owned(),
                                                        Storage::Public,
                                                        data,
                                                        WriteMtime::No,
                                                    )
                                                    .await
                                                    .map_err(|_| {
                                                        ApplicationError::StoragePermissions
                                                    })?;
                                                } else {
                                                    // `TODO`: log check
                                                    warn!("Got Modify event, but resource doesn't exist. Reconnecting might help, but this could be an extorsion.");
                                                };
                                            } else {
                                                // do nothing, as the doc says
                                            }
                                        }
                                        agde::EventKind::Create(ev) => {
                                            (options.write)(
                                                ev.resource().to_owned(),
                                                Storage::Public,
                                                Vec::new(),
                                                WriteMtime::No,
                                                // agde::event::dur_to_systime(event.timestamp()),
                                                // None,
                                            )
                                            .await
                                            .map_err(|()| ApplicationError::StoragePermissions)?;
                                        }
                                        agde::EventKind::Delete(ev) => {
                                            (options.delete)(
                                                ev.resource().to_owned(),
                                                Storage::Public,
                                            )
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
            let mut last_check = SystemTime::now();
            loop {
                let diff = (options.rough_resource_diff)()
                    .await
                    .map_err(|_| ApplicationError::StoragePermissions)?;
                let mut messages = Vec::with_capacity(diff.len());
                {
                    let mut manager = manager.lock().await;

                    for diff in diff {
                        let modern = manager.modern_resource_name(diff.resource(), last_check);
                        // It's just not worth sending updates when the resource has been deleted.
                        if modern.is_some() {
                            let event = match diff {
                                // `TODO`: Give successor
                                Change::Delete(res) => agde::Event::with_timestamp(
                                    agde::event::Kind::<agde::VecSection>::Delete(
                                        agde::event::Delete::new(res, None),
                                    ),
                                    manager.uuid(),
                                    last_check,
                                ),
                                Change::Modify(res, created) => {
                                    if created {
                                        let event = agde::Event::with_timestamp(
                                            agde::event::Kind::<agde::VecSection>::Create(
                                                agde::event::Create::new(res.clone()),
                                            ),
                                            manager.uuid(),
                                            last_check,
                                        );
                                        messages.push(manager.process_event(event));
                                    }
                                    let data = (options.read)(res.clone(), Storage::Current)
                                        .await
                                        .map_err(|_| ApplicationError::StoragePermissions)?.expect("configuration should not return Modified if the Current storage version doesn't exist.");
                                    let len = data.len();
                                    let mut base = (options.read)(res.clone(), Storage::Public)
                                        .await
                                        .map_err(|_| ApplicationError::StoragePermissions)?
                                        .unwrap_or_default();

                                    let mut unwinder = manager.unwinder_to(last_check);
                                    let mut base_slice = agde::SliceBuf::with_whole(&mut base);
                                    base_slice.extend_to_needed(unwinder.sections(&res).expect("error when unwinding public storage. Resource name is valid."), 0);
                                    unwinder.unwind(&mut base_slice, &res).expect("error when unwinding public storage. Resource name is valid and capacity is checked.");

                                    let event = agde::event::Modify::new(
                                        res,
                                        vec![agde::VecSection::whole_resource(len, data)],
                                        Some(&base),
                                    );

                                    // let mut base_slice = agde::SliceBuf::with_whole(&mut base);
                                    // base_slice.extend_to_needed(event.sections(), 0);

                                    // for section in event.sections() {
                                    // section.apply(&mut base_slice).expect("we've use agde's functions for guaranteeing slice capacity.");
                                    // }

                                    // // doesn't work, since agde changes the log to fit our changes
                                    // // (later changes and their positions)
                                    // unwinder.rewind(resource)

                                    agde::Event::with_timestamp(
                                        agde::event::Kind::Modify(event),
                                        manager.uuid(),
                                        last_check,
                                    )
                                }
                            };

                            messages.push(manager.process_event(event))
                        } else {
                            // edited resource has been removed.
                        }
                    }
                }

                {
                    let mut manager = manager.lock().await;

                    for message in &messages {
                        let event = if let agde::MessageKind::Event(ev) = message.inner() {
                            ev
                        } else {
                            unreachable!("we only added messages through `process_event`, which always gives events.");
                        };

                        info!("Processing sent message. : {event:?}");

                        let applier = manager
                            .apply_event(event, message.uuid())
                            .expect("manager failed to accept our own event");

                        match applier.event().inner() {
                            agde::EventKind::Modify(ev) => {
                                if let Some(resource) = applier.resource() {
                                    let mut resource_data =
                                        (options.read)(resource.to_owned(), Storage::Public)
                                            .await
                                            .map_err(|()| ApplicationError::StoragePermissions)?
                                            // don't expect, just make a new file.
                                            .expect("we trust our own data - there must have been a create event before modify");
                                    let mut slice = agde::SliceBuf::with_whole(&mut resource_data);
                                    slice.extend_to_needed(ev.sections(), 0);
                                    applier.apply(&mut slice).unwrap();
                                    let len = slice.filled().len();
                                    resource_data.truncate(len);
                                    (options.write)(
                                        resource.to_owned(),
                                        Storage::Public,
                                        resource_data,
                                        WriteMtime::LookUpCurrent,
                                    )
                                    .await
                                    .map_err(|_| ApplicationError::StoragePermissions)?;
                                } else {
                                    // do nothing, as the doc says
                                }
                            }
                            agde::EventKind::Create(ev) => {
                                (options.write)(
                                    ev.resource().to_owned(),
                                    Storage::Public,
                                    Vec::new(),
                                    WriteMtime::LookUpCurrent,
                                    // agde::event::dur_to_systime(event.timestamp()),
                                    // Some
                                    // None,
                                )
                                .await
                                .map_err(|()| ApplicationError::StoragePermissions)?;
                            }
                            agde::EventKind::Delete(ev) => {
                                info!("Processing local delete message.");
                                (options.delete)(ev.resource().to_owned(), Storage::Public)
                                    .await
                                    .map_err(|()| ApplicationError::StoragePermissions)?;
                            }
                        }
                    }

                    info!("Last check {:?}", last_check.elapsed());
                    // // move changed files to [`Storage::Current`].
                    // let unwinder = manager.unwinder_to(last_check);
                    // info!("Events {:?}", unwinder.events().collect::<Vec<_>>());
                    // // Dedup if a resource has been modified several times during this task's
                    // // sleep.
                    // let affected_resources = {
                    // let mut set = HashSet::new();
                    // for ev in unwinder.events() {
                    // if ev.sender() != manager.uuid() {
                    // set.insert(ev.resource());
                    // }
                    // // if let Some(latest_change) = map.get_mut(ev.resource()) {
                    // // *latest_change = (*latest_change).max(ev.timestamp());
                    // // } else {
                    // // map.insert(ev.resource().to_owned(), ev.timestamp());
                    // // }
                    // }
                    // set
                    // };
                    debug!("Processed messages. Moving from public to current.");
                    {
                        let mut changes = changed.lock().await;
                        for resource in &*changes {
                            warn!("Resource {resource} changed, from **remote**");
                            let actual = (options.read)(resource.to_owned(), Storage::Public)
                                .await
                                .map_err(|_| ApplicationError::StoragePermissions)?;
                            if let Some(actual) = actual {
                                (options.write)(
                                    resource.to_owned(),
                                    Storage::Current,
                                    actual,
                                    WriteMtime::LookUpCurrent,
                                    // agde::event::dur_to_systime(latest_change),
                                )
                                .await
                                .map_err(|_| ApplicationError::StoragePermissions)?;
                            } else {
                                (options.delete)(resource.to_owned(), Storage::Current)
                                    .await
                                    .map_err(|_| ApplicationError::StoragePermissions)?;
                            }
                        }
                        changes.clear();
                    }
                    debug!("Successfully applied diffs.");
                }
                {
                    let mut write = write.lock().await;
                    for message in &messages {
                        let message = message.to_bin().into();

                        write
                            .feed(message)
                            .await
                            .map_err(|_| ApplicationError::UnexpectedServerClose)?;
                    }
                    write
                        .flush()
                        .await
                        .map_err(|_| ApplicationError::UnexpectedServerClose)?;
                    debug!("Successfully sent diffs.");
                }

                last_check = SystemTime::now();
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
    let metadata = tokio::fs::read(".agde/metdata")
        .then(|r| async move { r.map_err(|_| ()) })
        .and_then(|data| async move {
            bincode::serde::decode_from_slice(
                &data,
                bincode::config::standard().write_fixed_array_length(),
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
