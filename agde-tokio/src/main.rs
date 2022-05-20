use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt::{self, Display};
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use std::{io, process};

use agde::fast_forward::{Metadata, MetadataChange, ResourceMeta};
use agde::Manager;
use futures::{Future, FutureExt, SinkExt, StreamExt, TryFutureExt};
use log::{debug, error, info, warn};
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

async fn metadata_new(storage: Storage) -> Result<Metadata, io::Error> {
    let map: Result<HashMap<String, ResourceMeta>, io::Error> =
        tokio::task::spawn_blocking(move || {
            let mut map = HashMap::new();

            let path = match storage {
                Storage::Public | Storage::Meta => ".agde/",
                Storage::Current => "./",
            };

            for entry in walkdir::WalkDir::new(path)
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
                        ResourceMeta::new(Some(modified), metadata.len()),
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

    Ok(Metadata::new(map))
}
async fn metadata_sync(metadata: &Metadata, name: &str) -> Result<(), io::Error> {
    let data = bincode::serde::encode_to_vec(
        metadata,
        bincode::config::standard().write_fixed_array_length(),
    )
    .expect("serialization of metadata should be infallible");
    tokio::fs::write(format!(".agde/{name}"), data).await?;
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Storage {
    /// The copy of data which is maintained to be equal to the others' public storages.
    Public,
    /// The copy of data the user writes to.
    Current,
    /// Storage of metadata objects.
    Meta,
}
pub enum WriteMtime {
    LookUpCurrent,
    No,
}

pub type BoxFut<T> = Pin<Box<dyn Future<Output = T> + Send>>;
pub type ReadFuture = BoxFut<Result<Option<Vec<u8>>, ()>>;
pub type WriteFuture = BoxFut<Result<(), ()>>;
pub type DeleteFuture = WriteFuture;
pub type DiffFuture = BoxFut<Result<Vec<MetadataChange>, ()>>;
pub type ReadFn = Box<dyn Fn(String, Storage) -> ReadFuture + Send + Sync>;
/// The [`SystemTime`] is the time of modification.
/// When [`Storage::Current`], this should be the time of last change.
pub type WriteFn = Box<dyn Fn(String, Storage, Vec<u8>, WriteMtime) -> WriteFuture + Send + Sync>;
pub type DeleteFn = Box<dyn Fn(String, Storage) -> DeleteFuture + Send + Sync>;
pub type DiffFn = Box<dyn Fn() -> DiffFuture + Send + Sync>;
#[must_use]
pub struct Options {
    pub read: ReadFn,
    pub write: WriteFn,
    pub delete: DeleteFn,
    /// Returns a list of the resources which might have changed.
    pub rough_resource_diff: DiffFn,

    offline_metadata: Arc<Mutex<Metadata>>,
    metadata: Arc<Mutex<Metadata>>,

    /// For how long to wait for welcomes.
    pub startup_timeout: Duration,
    pub sync_interval: Duration,

    pub force_pull: bool,
}
// `TODO`: Add option to write new resource changes to `Current` if that resource hasn't been
// changed in current.
impl Options {
    pub async fn fs(force_pull: bool) -> Result<Self, io::Error> {
        let metadata =
            initial_metadata("metadata", || metadata_new(Storage::Public), force_pull).await?;
        // A metadata cache that is held constant between diff calls.
        let offline_metadata = initial_metadata(
            "metadata-offline",
            || {
                let r = Ok(metadata.clone());
                futures::future::ready(r)
            },
            force_pull,
        )
        .await?;

        let metadata = Arc::new(Mutex::new(metadata));
        let offline_metadata = Arc::new(Mutex::new(offline_metadata));
        let write_metadata = Arc::clone(&metadata);
        let write_offline_metadata = Arc::clone(&offline_metadata);
        let delete_metadata = Arc::clone(&metadata);
        let delete_offline_metadata = Arc::clone(&offline_metadata);
        let diff_metadata = Arc::clone(&metadata);
        let diff_offline_metadata = Arc::clone(&offline_metadata);
        Ok(Options {
            read: Box::new(|resource, storage| {
                Box::pin(async move {
                    let path = match storage {
                        Storage::Public => format!(".agde/files/{resource}",),
                        Storage::Current => format!("./{resource}"),
                        Storage::Meta => format!(".agde/{resource}"),
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
                }) as ReadFuture
            }),
            write: Box::new(move |resource, storage, data, mtime| {
                let metadata = Arc::clone(&write_metadata);
                let offline_metadata = Arc::clone(&write_offline_metadata);
                Box::pin(async move {
                    let path = match storage {
                        Storage::Public => format!(".agde/files/{resource}",),
                        Storage::Current => format!("./{resource}"),
                        Storage::Meta => format!(".agde/{resource}"),
                    };

                    if let Some(path) = Path::new(&path).parent() {
                        tokio::fs::create_dir_all(path).await.map_err(|_| ())?;
                    }
                    info!("Writing to {path}, {:?}", String::from_utf8_lossy(&data));

                    let mut file = tokio::fs::File::create(&path).await.map_err(|_| ())?;
                    file.write_all(&data).await.map_err(|_| ())?;
                    file.flush().await.map_err(|_| ())?;
                    match storage {
                        // meta storage obviously doesn't affect the files' metadata.
                        Storage::Meta => {}
                        Storage::Public => {
                            let mut metadata = metadata.lock().await;

                            let mtime = match mtime {
                                WriteMtime::No => None,
                                WriteMtime::LookUpCurrent => {
                                    if let Ok(metadata) =
                                        tokio::fs::metadata(format!("./{resource}")).await
                                    {
                                        let mtime = metadata.modified().map_err(|_| ())?;
                                        Some(mtime)
                                    } else {
                                        None
                                    }
                                }
                            };
                            let meta = ResourceMeta::new(mtime, data.len() as u64);
                            metadata.insert(resource, meta);
                        }
                        Storage::Current => {
                            let file_metadata = tokio::fs::metadata(&path).await.map_err(|_| ())?;
                            let mtime = file_metadata.modified().map_err(|_| ())?;
                            {
                                let mut metadata = offline_metadata.lock().await;

                                metadata.insert(
                                    resource.clone(),
                                    ResourceMeta::new(Some(mtime), data.len() as u64),
                                );
                            }
                            {
                                let mut metadata = metadata.lock().await;

                                metadata.insert(
                                    resource,
                                    ResourceMeta::new(Some(mtime), data.len() as u64),
                                );
                                metadata_sync(&*metadata, "metadata")
                                    .await
                                    .map_err(|_| ())?;
                            }
                        }
                    }
                    Ok(())
                }) as WriteFuture
            }),
            delete: Box::new(move |resource, storage| {
                let metadata = Arc::clone(&delete_metadata);
                let offline_metadata = Arc::clone(&delete_offline_metadata);
                Box::pin(async move {
                    let path = match storage {
                        Storage::Public => format!(".agde/files/{resource}",),
                        Storage::Current => format!("./{resource}"),
                        Storage::Meta => format!(".agde/{resource}"),
                    };
                    if storage == Storage::Current {
                        let mut metadata = offline_metadata.lock().await;
                        debug!("Removing {resource} from metdata cache.");
                        metadata.remove(&resource);
                    }
                    {
                        let mut metadata = metadata.lock().await;
                        debug!("Removing {resource} from metdata cache.");
                        metadata.remove(&resource);
                        metadata_sync(&*metadata, "metadata")
                            .await
                            .map_err(|_| ())?;
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
                }) as DeleteFuture
            }),
            rough_resource_diff: Box::new(move || {
                let metadata = Arc::clone(&diff_metadata);
                let offline_metadata = Arc::clone(&diff_offline_metadata);
                Box::pin(async move {
                    debug!("Getting diff");
                    let mut offline_metadata = offline_metadata.lock().await;
                    let current_metadata = metadata_new(Storage::Current).await.map_err(|_| ())?;
                    let changed = offline_metadata.changes(&current_metadata);
                    let mut metadata = metadata.lock().await;
                    for change in &changed {
                        match change {
                            MetadataChange::Modify(res, _) => {
                                metadata.insert(res.clone(), current_metadata.get(res).unwrap());
                            }
                            MetadataChange::Delete(res) => {
                                metadata.remove(res);
                            }
                        }
                    }
                    {
                        // `TODO`: Optimize this
                        *offline_metadata = metadata.clone();
                        futures::future::try_join(
                            Box::pin(metadata_sync(&metadata, "metadata")),
                            Box::pin(metadata_sync(&offline_metadata, "metadata-offline")),
                        )
                        .await
                        .map_err(|_| ())?;
                    }
                    debug!("Changed: {:?}", changed);
                    Ok(changed)
                }) as DiffFuture
            }),
            metadata,
            offline_metadata,
            startup_timeout: Duration::from_secs(7),
            sync_interval: Duration::from_secs(5),
            force_pull,
        })
    }
    pub fn arc(self) -> Arc<Self> {
        Arc::new(self)
    }
    pub fn with_startup_duration(mut self, startup_timeout: Duration) -> Self {
        self.startup_timeout = startup_timeout;
        self
    }
    /// The metadata of both the public and current storage.
    /// Calling [`Metadata::changes`] on `this.changes(offline_metadata)`
    /// gets you the changes to get current storage the same as the public.
    pub fn metadata(&self) -> &Mutex<Metadata> {
        &self.metadata
    }
    /// The metadata of the [`Storage::Current`].
    pub fn metadata_offline(&self) -> &Mutex<Metadata> {
        &self.offline_metadata
    }
    pub async fn read(
        &self,
        resource: impl Into<String>,
        storage: Storage,
    ) -> Result<Option<Vec<u8>>, ApplicationError> {
        (self.read)(resource.into(), storage)
            .await
            .map_err(|_| ApplicationError::StoragePermissions)
    }
    pub async fn write(
        &self,
        resource: impl Into<String>,
        storage: Storage,
        data: impl Into<Vec<u8>>,
        write_mtime: WriteMtime,
    ) -> Result<(), ApplicationError> {
        (self.write)(resource.into(), storage, data.into(), write_mtime)
            .await
            .map_err(|_| ApplicationError::StoragePermissions)
    }
    pub async fn delete(
        &self,
        resource: impl Into<String>,
        storage: Storage,
    ) -> Result<(), ApplicationError> {
        (self.delete)(resource.into(), storage)
            .await
            .map_err(|_| ApplicationError::StoragePermissions)
    }
    /// The rough diff calculated by the difference between the metadata collections.
    pub async fn diff(&self) -> Result<Vec<MetadataChange>, ApplicationError> {
        (self.rough_resource_diff)()
            .await
            .map_err(|_| ApplicationError::StoragePermissions)
    }
}

#[tokio::main]
async fn main() {
    env_logger::init();

    // `TODO`: use clap for argument parsing.
    let url = "ws://localhost:8081/ws";

    loop {
        let mut options = Options::fs(false)
            .await
            .expect("failed to read file system metadata");
        options.startup_timeout = Duration::from_secs(1);
        let options = options.arc();

        let log_lifetime = Duration::from_secs(60);

        if log_lifetime <= options.sync_interval * 2 {
            error!("Increase frequency of sync or increase log lifetime.");
        }

        let manager = Manager::new(false, 0, log_lifetime, 512);

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
    let state = options.read("clean", Storage::Meta).await?;

    if state.as_deref() != Some(b"y") {
        error!("State isn't clean.");

        let changes = options.diff().await?;
        if options.force_pull {
            if !changes.is_empty() {
                error!("Overriding local changes since no state file was found.");
            }

            for change in changes {
                let resource = match change {
                    MetadataChange::Modify(resource, _) | MetadataChange::Delete(resource) => {
                        resource
                    }
                };
                let actual = options.read(&resource, Storage::Public).await?;
                if let Some(actual) = actual {
                    options
                        .write(
                            resource,
                            Storage::Current,
                            actual,
                            WriteMtime::LookUpCurrent,
                        )
                        .await?;
                }
            }

            options
                .write("clean", Storage::Meta, "y", WriteMtime::No)
                .await?;
        } else if changes.is_empty() {
            options
                .write("clean", Storage::Meta, "y", WriteMtime::No)
                .await?;
        }
    }

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

    // `TODO`: do this outside of run, as we shouldn't call this several times.
    // It's also system specific.
    {
        let manager = Arc::clone(&manager);
        let options = Arc::clone(&options);
        let handler = ctrlc::set_handler(move || {
            info!("Caught ctrlc");
            let manager = Arc::clone(&manager);
            let options = Arc::clone(&options);
            let runtime = tokio::runtime::Builder::new_current_thread()
                .max_blocking_threads(1)
                .build()
                .expect("failed to start tokio when handling ctrlc");
            let returned = runtime.block_on(async move {
                let clean = options.read("clean", Storage::Meta).await?;
                if clean.as_deref() == Some(b"y") {
                    info!("State clean. Exiting.");
                    return Ok(());
                }
                error!("State not clean. Trying to apply diffs to current buffer.");

                let mut manager = manager.lock().await;

                let diff = options.diff().await?;

                {
                    for diff in diff {
                        let modern = manager.modern_resource_name(
                            diff.resource(),
                            manager.last_commit().unwrap_or(SystemTime::UNIX_EPOCH),
                        );
                        if modern.is_some() {
                            match diff {
                                MetadataChange::Delete(_) => {}
                                MetadataChange::Modify(resource, created) => {
                                    let mut current =
                                        options.read(&resource, Storage::Current).await?.expect(
                                            "configuration should not return Modified \
                                            if the Current storage version doesn't exist.",
                                        );

                                    let public = options
                                        .read(&resource, Storage::Public)
                                        .await?
                                        .unwrap_or_default();

                                    current = if let Some(current) = rewind_current(
                                        &mut *manager,
                                        created,
                                        &resource,
                                        &public,
                                        &current,
                                    )
                                    .await
                                    {
                                        current
                                    } else {
                                        // since we keep track of the incoming events and
                                        // which resources have been changed, this will get
                                        // copied below.
                                        continue;
                                    };

                                    options
                                        .write(resource, Storage::Current, current, WriteMtime::No)
                                        .await?;
                                }
                            }
                        }
                    }
                }

                // we are clean again!
                options
                    .write("clean", Storage::Meta, "y", WriteMtime::No)
                    .await?;

                Ok::<(), ApplicationError>(())
            });

            if let Err(err) = returned {
                error!("Error on ctrlc cleanup: {err:?}");
            }

            info!("Successfully cleaned up.");
            std::process::exit(0);
        });
        if handler.is_err() {
            warn!("Failed to set ctrlc handler.");
        };
    }

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
                        let mut message = if let Ok(m) = message {
                            m
                        } else {
                            warn!(
                                "Received invalid binary message. A pier might be faulty. Data: {}",
                                String::from_utf8_lossy(&data)
                            );
                            continue;
                        };

                        let mut manager = manager.lock().await;
                        match message.recipient() {
                            agde::Recipient::All => {}
                            agde::Recipient::Selected(recipient) => {
                                if recipient.uuid() != manager.uuid() {
                                    continue;
                                }
                            }
                        }
                        let sender = message.sender();
                        let message_uuid = message.uuid();
                        match message.inner_mut() {
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
                                info!(
                                    "Got event from pier {}: {:?} {event:?}",
                                    sender,
                                    SystemTime::now()
                                );
                                {
                                    changed.lock().await.insert(event.resource().to_owned());
                                }

                                match manager.apply_event(event, message_uuid) {
                                    Ok(mut applier) => {
                                        let resource = applier.resource();

                                        if let Some(resource) = resource {
                                            // write to `.agde/clean` that we aren't clean (we have
                                            // public diffs not applied to `current`)
                                            options
                                                .write("clean", Storage::Meta, "n", WriteMtime::No)
                                                .await?;

                                            match applier.event().inner() {
                                                agde::EventKind::Modify(_) => {
                                                    let resource_data = options
                                                        .read(resource, Storage::Public)
                                                        .await?;

                                                    if let Some(mut data) = resource_data {
                                                        info!(
                                                            "Read {:?} from public",
                                                            String::from_utf8_lossy(&data)
                                                        );

                                                        let resource = resource.to_owned();
                                                        data = applier.apply(&data).unwrap();
                                                        options
                                                            .write(
                                                                resource,
                                                                Storage::Public,
                                                                data,
                                                                WriteMtime::No,
                                                            )
                                                            .await?;
                                                    } else {
                                                        // `TODO`: log check
                                                        warn!("Got Modify event, but resource doesn't exist. Reconnecting might help, but this could be an extortion to attempt to make you disconnect.");
                                                    };
                                                }
                                                agde::EventKind::Create(_) => {
                                                    options
                                                        .write(
                                                            resource,
                                                            Storage::Public,
                                                            Vec::new(),
                                                            WriteMtime::No,
                                                        )
                                                        .await?;
                                                }
                                                agde::EventKind::Delete(_) => {
                                                    options
                                                        .delete(resource, Storage::Public)
                                                        .await?;
                                                }
                                            }
                                        } else {
                                            // do nothing, as the doc says
                                        }
                                    }
                                    Err(err) => {
                                        warn!("Slow pier. Got error from internal log: {err:?}. Running a log check.");
                                        // `TODO`: Log check!
                                    }
                                };
                            }
                            // `TODO`: handle cancelled fast forwards
                            agde::MessageKind::FastForward(_ff) => {
                                let meta = options.metadata().lock().await;
                                let msg = manager
                                    .process_fast_forward_response(meta.clone(), message.sender());
                                send(&write, msg).await?;
                            }
                            agde::MessageKind::FastForwardReply(ff) => {
                                let mut sync_request =
                                    match manager.apply_fast_forward_reply(ff, sender) {
                                        Ok(v) => v,
                                        Err(agde::fast_forward::Error::UnexpectedPier) => continue,
                                        e => e.unwrap(),
                                    };
                                let changes =
                                    { options.metadata().lock().await.changes(ff.metadata()) };
                                println!("Changes: {changes:?}");
                                for change in changes {
                                    match change {
                                        MetadataChange::Modify(res, _created) => {
                                            let data = options
                                                .read(&res, Storage::Public)
                                                .await?
                                                .unwrap_or_default();
                                            let mut sig = agde::den::Signature::with_algorithm(
                                                agde::den::HashAlgorithm::XXH3_64,
                                                128,
                                            );
                                            sig.write(&data);
                                            let sig = sig.finish();
                                            sync_request.insert(res, sig);
                                        }
                                        MetadataChange::Delete(res) => {
                                            options.delete(res, Storage::Public).await?;
                                        }
                                    }
                                }

                                let sync_request = sync_request.finish();
                                let msg = manager.process_sync(sync_request);
                                send(&write, msg).await?;
                            }
                            agde::MessageKind::Sync(sync) => {
                                let mut builder = manager.apply_sync(sync, sender);
                                while let Some((resource, signature)) = builder.next_signature() {
                                    let data = options.read(resource, Storage::Public).await?;
                                    let data = if let Some(d) = data {
                                        d
                                    } else {
                                        continue;
                                    };
                                    let diff = signature.diff(&data);
                                    // so we don't call builder ↓, as that would be multiple
                                    // mutable borrows.
                                    let resource = resource.to_owned();
                                    builder.add_diff(resource, diff);
                                }
                                let msg = manager.process_sync_reply(builder);
                                send(&write, msg).await?;
                            }
                            agde::MessageKind::SyncReply(sync) => {
                                println!("Sync reply: {:?}", sync);
                                let mut changed = changed.lock().await;

                                let mut rewinder = manager.apply_sync_reply(sync).unwrap();

                                for resource in sync.delete() {
                                    let resource = resource.as_ref();
                                    {
                                        changed.insert(resource.to_owned());
                                    }
                                    options.delete(resource, Storage::Public).await?;
                                }
                                for (resource, diff) in sync.diff() {
                                    let resource = resource.as_ref();
                                    {
                                        changed.insert(resource.to_owned());
                                    }
                                    let public = async {
                                        let mut data = options
                                            .read(resource, Storage::Public)
                                            .await?
                                            .unwrap_or_default();
                                        println!("Read {:?}", std::str::from_utf8(&data));

                                        if diff.apply_overlaps(data.len()) {
                                            let mut other = Vec::with_capacity(data.len() + 64);
                                            diff.apply(&data, &mut other).unwrap();
                                            data = other;
                                        } else {
                                            diff.apply_in_place(&mut data).unwrap();
                                        }
                                        println!("applied {:?}", std::str::from_utf8(&data));
                                        data = rewinder.rewind(resource, data).unwrap();
                                        println!("rewound {:?}", std::str::from_utf8(&data));
                                        options
                                            .write(resource, Storage::Public, data, WriteMtime::No)
                                            .await?;
                                        Ok(())
                                    };
                                    let current = async {
                                        let mut data = options
                                            .read(resource, Storage::Current)
                                            .await?
                                            .unwrap_or_default();

                                        if diff.apply_overlaps_adaptive_end(data.len()) {
                                            let mut other = Vec::with_capacity(data.len() + 64);
                                            diff.apply_adaptive_end(&data, &mut other).unwrap();
                                            data = other;
                                        } else {
                                            diff.apply_in_place_adaptive_end(&mut data).unwrap();
                                        }
                                        options
                                            .write(resource, Storage::Current, data, WriteMtime::No)
                                            .await?;
                                        Ok(())
                                    };
                                    futures::future::try_join(public, current).await?;
                                }
                            }
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
        let manager = Arc::clone(&manager);
        let write = Arc::clone(&write);
        let changed = Arc::clone(&changed);
        tokio::spawn(async move {
            loop {
                commit_and_send(&manager, &options, &write, &changed).await?;
                tokio::time::sleep(options.sync_interval).await;
            }
        })
    };

    {
        let mut manager = manager.lock().await;
        if let Some(msg) = manager.process_fast_forward() {
            let mut write = write.lock().await;
            write.send(msg.to_bin().into()).await?;
        }
    }

    // `TODO`: periodic calls (`clean_event_uuid_log_checks`, periodic (hash, event log) checks)

    let result = match futures::future::select(accept_handle, local_watcher).await {
        futures::future::Either::Left((result, other))
        | futures::future::Either::Right((result, other)) => {
            other.abort();
            result.expect("task panicked")
        }
    };
    result.map_err(|err| Box::new(err) as DynError)
}

/// Continue if this return [`None`] - then the resource is destroyed.
async fn rewind_current(
    manager: &mut Manager,
    created: bool,
    resource: &str,
    public: &[u8],
    current: &[u8],
) -> Option<Vec<u8>> {
    let last_commit = manager.last_commit_or_epoch();
    let offsets = if !created {
        let mut unwinder = manager.unwinder_to(last_commit);

        // here, `resource` is just the modern name. See the code
        // above for more info
        let unwound_public = unwinder.unwind(public, resource).expect(
            "error when unwinding public storage. Resource name is valid and capacity is checked.",
        );

        let old_diff = agde::event::diff(&unwound_public, current);

        println!("  OLD DIFF {old_diff:?}");

        let mut offsets = agde::utils::Offsets::new();
        offsets.add_diff(
            &old_diff,
            agde::utils::sub_usize(current.len(), unwound_public.len()),
        );
        offsets
    } else {
        agde::utils::Offsets::new()
    };

    let mut rewinder = manager.rewind_from_last_commit();

    match rewinder.rewind_with_modify_diff(resource, current, |diff| {
        let mut diff = diff.clone();
        let original_data_len = diff.original_data_len();
        offsets.apply_single(&mut diff);
        // ignore `offsets`'s changes, as `apply_adaptive_end`
        // takes care of that.
        diff.set_original_data_len(original_data_len);
        println!("Diff changed with {offsets:?}, now {diff:?}");
        Cow::Owned(diff)
    }) {
        Err(agde::event::RewindError::ResourceDestroyed(_)) => None,
        Err(agde::event::RewindError::ApplyError(err)) => {
            Err(err).expect("error when applying diffs to current")
        }
        Ok(vec) => Some(vec),
    }
}
async fn commit_and_send(
    manager: &Mutex<Manager>,
    options: &Options,
    write: &Mutex<WriteHalf>,
    changed: &Mutex<HashSet<String>>,
) -> Result<(), ApplicationError> {
    {
        let ff = { manager.lock().await.is_fast_forwarding() };
        if ff {
            return Ok(());
        }
    }

    let changes = options.diff().await?;

    info!("Got diffs {changes:?}");

    let mut messages = Vec::with_capacity(changes.len());

    {
        let mut manager = manager.lock().await;

        for diff in changes {
            let modern =
                manager.modern_resource_name(diff.resource(), manager.last_commit_or_epoch());
            // It's not worth sending updates when the resource has been deleted.
            if modern.is_some() {
                let event = match diff {
                    // `TODO`: Give successor
                    MetadataChange::Delete(res) => agde::Event::new(
                        agde::event::Kind::Delete(agde::event::Delete::new(res, None)),
                        &*manager,
                    ),
                    MetadataChange::Modify(resource, created) => {
                        let create_ev = if created {
                            let event = agde::Event::new(
                                agde::event::Kind::Create(agde::event::Create::new(
                                    resource.clone(),
                                )),
                                &manager,
                            )
                            // schedule create event a bit before modify.
                            .with_timestamp(SystemTime::now() - Duration::from_micros(10));
                            Some(event)
                        } else {
                            None
                        };

                        let mut current = options.read(&resource, Storage::Current).await?.expect(
                            "configuration should not return Modified \
                                if the Current storage version doesn't exist.",
                        );

                        let public = options
                            .read(&resource, Storage::Public)
                            .await?
                            .unwrap_or_default();

                        info!("Read {:?} from public", String::from_utf8_lossy(&public));
                        info!(
                            "Last check: {:?}, now {:?}",
                            manager.last_commit_or_epoch(),
                            SystemTime::now()
                        );

                        current = if let Some(current) =
                            rewind_current(&mut manager, created, &resource, &public, &current)
                                .await
                        {
                            current
                        } else {
                            // since we keep track of the incoming events and
                            // which resources have been changed, this will get
                            // copied below.
                            println!("Resource destroyed");
                            continue;
                        };

                        warn!(
                            "Unwould public data, now resource at: '''\n{:?}\n'''",
                            std::str::from_utf8(&public)
                        );

                        let event = agde::event::Modify::new(resource, &current, &public);
                        if !event.diff().is_empty() {
                            let event =
                                agde::Event::new(agde::event::Kind::Modify(event), &manager);

                            println!("Diff: {:#?}", event.diff().unwrap());

                            if let Some(ev) = create_ev {
                                messages.push(manager.process_event(ev).unwrap());
                            }
                            event
                        } else {
                            if let Some(ev) = create_ev {
                                messages.push(manager.process_event(ev).unwrap());
                            }
                            continue;
                        }
                    }
                };

                messages.push(manager.process_event(event).unwrap())
            } else {
                // edited resource has been removed.
            }
        }

        manager.update_last_commit();
    }

    // Execute `apply` and `send` at the same time!
    let apply = async {
        let mut manager = manager.lock().await;

        for message in &messages {
            let event = if let agde::MessageKind::Event(ev) = message.inner() {
                ev
            } else {
                unreachable!(
                    "we only added messages through `process_event`, which always gives events."
                );
            };

            debug!("Processing sent message: {event:?}");

            let mut applier = manager
                .apply_event(event, message.uuid())
                .expect("manager failed to accept our own event");

            let resource = applier
                .resource()
                .expect("our own messages are too old")
                .to_owned();

            match applier.event().inner() {
                agde::EventKind::Modify(_ev) => {
                    let mut resource_data = options.read(&resource, Storage::Public).await?.expect(
                        "we trust our own data - there must \
                            have been a create event before modify",
                    );

                    resource_data = applier.apply(&resource_data).unwrap();

                    options
                        .write(
                            resource,
                            Storage::Public,
                            resource_data,
                            WriteMtime::LookUpCurrent,
                        )
                        .await?;
                }
                agde::EventKind::Create(_) => {
                    options
                        .write(resource, Storage::Public, "", WriteMtime::LookUpCurrent)
                        .await?;
                }
                agde::EventKind::Delete(_) => {
                    info!("Processing local delete message.");
                    options.delete(resource, Storage::Public).await?;
                }
            }
        }

        debug!("Processed messages. Moving from public to current.");
        {
            let mut changes = changed.lock().await;
            for resource in &*changes {
                warn!("Resource {resource} changed, from **remote**");
                let actual = options.read(resource, Storage::Public).await?;
                if let Some(actual) = actual {
                    options
                        .write(
                            resource,
                            Storage::Current,
                            actual,
                            WriteMtime::LookUpCurrent,
                        )
                        .await?;
                } else {
                    options.delete(resource, Storage::Current).await?;
                }
            }
            changes.clear();
        }
        debug!("Successfully applied diffs.");
        Ok(())
    };
    // Execute `apply` and `send` at the same time!
    let send = async {
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
        Ok(())
    };
    futures::future::try_join(apply, send).await?;
    {
        options
            .write("clean", Storage::Meta, "y", WriteMtime::No)
            .await?;
    }
    Ok(())
}

async fn connect_ws(url: &str) -> Result<(WriteHalf, ReadHalf), DynError> {
    info!("Connecting to {url:?}.");
    let result = tokio_tungstenite::connect_async(url).await;
    let conenction = result?;
    Ok(conenction.0.split())
}
async fn initial_metadata<F: Future<Output = Result<Metadata, io::Error>>>(
    name: &str,
    new: impl Fn() -> F,
    force_pull: bool,
) -> Result<Metadata, io::Error> {
    tokio::fs::create_dir_all(".agde").await?;
    let metadata = tokio::fs::read(format!(".agde/{name}"))
        .then(|r| async move { r.map_err(|_| ()) })
        .and_then(|data| async move {
            bincode::serde::decode_from_slice::<Metadata, _>(
                &data,
                bincode::config::standard().write_fixed_array_length(),
            )
            .map_err(|_| ())
        });
    match metadata.await {
        Ok((metadata, _)) => {
            let mut metadata: Metadata = metadata;
            let mut differing = Vec::new();
            for (resource, metadata) in metadata.iter() {
                let meta = tokio::fs::metadata(format!(".agde/files/{resource}")).await;
                match meta {
                    Ok(meta) => {
                        if meta.len() != metadata.size() {
                            differing.push(resource.to_owned());
                            error!("File {resource} has different length in cached metadata and on disk.");
                        }
                    }
                    Err(err) => match err.kind() {
                        io::ErrorKind::NotFound => {
                            differing.push(resource.to_owned());
                            error!("File {resource} is not found on disk.");
                        }
                        _ => return Err(err),
                    },
                };
            }

            for resource in differing {
                metadata.remove(&resource);
            }

            Ok(metadata)
        }
        Err(_) => {
            error!("Metadata corrupt. Recreating.");

            let populated = tokio::task::spawn_blocking(move || {
                walkdir::WalkDir::new("./")
                    .follow_links(false)
                    .into_iter()
                    .filter_entry(|e| {
                        !(e.path().starts_with(".agde") || e.path().starts_with("./.agde"))
                    })
                    .any(|e| {
                        e.as_ref()
                            .map_or(true, |e| e.metadata().map_or(true, |meta| meta.is_file()))
                    })
            })
            .await
            .unwrap();

            if !populated || force_pull {
                let metadata = new().await?;
                if let Err(err) = metadata_sync(&metadata, name).await {
                    error!("Failed to write newly created metadata: {err:?}");
                }
                Ok(metadata)
            } else {
                error!("Metadata not found. Directory is not empty. Refusing to override files.");
                Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    "refusing to override files",
                ))
            }
        }
    }
}

async fn send(stream: &Mutex<WriteHalf>, message: agde::Message) -> Result<(), ApplicationError> {
    stream
        .lock()
        .await
        .send(message.to_bin().into())
        .await
        .map_err(|_| ApplicationError::UnexpectedServerClose)
}
