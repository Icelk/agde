use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::convert::identity;
use std::error::Error;
use std::fmt::{self, Display};
use std::io;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use agde::fast_forward::{Metadata, MetadataChange, ResourceMeta};
use agde::Manager;
use futures::{Future, FutureExt, SinkExt, StreamExt, TryFutureExt};
use log::{debug, error, info, warn};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite;

pub mod native;

pub type DynError = Box<dyn Error>;
pub type WsStream =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;
pub type WriteHalf = futures::stream::SplitSink<WsStream, tungstenite::Message>;
pub type ReadHalf = futures::stream::SplitStream<WsStream>;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Storage {
    /// The copy of data which is maintained to be equal to the others' public storages.
    Public,
    /// The copy of data the user writes to.
    Current,
    /// Storage of metadata objects.
    Meta,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteStorage {
    /// The copy of data which is maintained to be equal to the others' public storages.
    ///
    /// See [`WriteFn`] and [`Options::write`] for more details on the data.
    Public(WriteMtime, SystemTime),
    /// The copy of data the user writes to.
    ///
    /// The [`WriteMtime`] signals if we should update the mtime.
    Current(WriteMtime),
    /// Storage of metadata objects.
    Meta,
}
impl WriteStorage {
    pub fn current() -> Self {
        Self::Current(WriteMtime::LookUpCurrent)
    }
    pub fn current_without_update() -> Self {
        Self::Current(WriteMtime::No)
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteMtime {
    LookUpCurrent,
    No,
}

pub type BoxFut<T> = Pin<Box<dyn Future<Output = T> + Send>>;
pub type ReadFuture = BoxFut<Result<Option<Vec<u8>>, ()>>;
pub type WriteFuture = BoxFut<Result<(), ()>>;
pub type DeleteFuture = WriteFuture;
pub type DiffFuture = BoxFut<Result<Vec<MetadataChange>, ()>>;
pub type SyncFuture = WriteFuture;
pub type ReadFn = Box<dyn Fn(String, Storage) -> ReadFuture + Send + Sync>;
/// The [`SystemTime`] is the timestamp of the event that caused this, or (if an event didn't cause
/// it) [`SystemTime::UNIX_EPOCH`].
/// Both the [`WriteMtime`] and [`SystemTime`] are useless unless [`Storage`] is [`Storage::Public`].
pub type WriteFn = Box<dyn Fn(String, WriteStorage, Vec<u8>) -> WriteFuture + Send + Sync>;
pub type DeleteFn = Box<dyn Fn(String, Storage) -> DeleteFuture + Send + Sync>;
pub type DiffFn = Box<dyn Fn() -> DiffFuture + Send + Sync>;
pub type SyncFn = Box<dyn Fn(Storage) -> SyncFuture + Send + Sync>;
#[must_use]
pub struct Options {
    read: ReadFn,
    write: WriteFn,
    delete: DeleteFn,
    /// Returns a list of the resources which might have changed.
    rough_resource_diff: DiffFn,
    sync_metadata: SyncFn,

    metadata: Arc<Mutex<Metadata>>,
    offline_metadata: Arc<Mutex<Metadata>>,
    file_cache: Arc<std::sync::Mutex<FileCache>>,

    /// For how long to wait for welcomes.
    startup_timeout: Duration,
    sync_interval: Duration,
    flush_interval: Duration,

    force_pull: bool,
    /// Verifies outgoing Modify events to be correct.
    /// A bit of a performance hit, but generally recommended.
    verify_diffs: bool,
}
// `TODO`: Add option to write new resource changes to `Current` if that resource hasn't been
// changed in current.
impl Options {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        read: ReadFn,
        write: WriteFn,
        delete: DeleteFn,
        rough_resource_diff: DiffFn,
        sync_metadata: SyncFn,
        metadata: Arc<Mutex<Metadata>>,
        offline_metadata: Arc<Mutex<Metadata>>,
        startup_timeout: Duration,
        sync_interval: Duration,
        force_pull: bool,
        verify_diffs: bool,
    ) -> Self {
        Self {
            read,
            write,
            delete,
            rough_resource_diff,
            sync_metadata,
            metadata,
            offline_metadata,
            file_cache: Arc::new(std::sync::Mutex::new(FileCache::new(1024 * 8))),
            startup_timeout,
            sync_interval,
            flush_interval: Duration::from_secs(5),
            force_pull,
            verify_diffs,
        }
    }
}
impl Options {
    pub fn arc(self) -> Arc<Self> {
        Arc::new(self)
    }
    pub fn with_startup_duration(mut self, startup_timeout: Duration) -> Self {
        self.startup_timeout = startup_timeout;
        self
    }
    pub fn with_sync_interval(mut self, sync_interval: Duration) -> Self {
        self.sync_interval = sync_interval;
        self
    }
    pub fn with_flush_interval(mut self, flush_interval: Duration) -> Self {
        self.flush_interval = flush_interval;
        self
    }
    pub fn with_file_cache_max_size(self, max_size: usize) -> Self {
        self.file_cache.lock().unwrap().max_size = max_size;
        self
    }
    #[must_use]
    pub fn sync_interval(&self) -> Duration {
        self.sync_interval
    }

    #[must_use]
    pub fn startup_timeout(&self) -> Duration {
        self.startup_timeout
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
}
impl Options {
    pub async fn read(
        &self,
        resource: impl Into<String> + AsRef<str>,
        storage: Storage,
    ) -> Result<Option<Vec<u8>>, ApplicationError> {
        match {
            let mut lock = self.file_cache.lock().unwrap();
            let v = lock.read_owned(resource, storage);
            drop(lock);
            v
        } {
            Ok(v) => v,
            Err(res) => self._read(res, storage).await,
        }
    }
    async fn _read(
        &self,
        resource: impl Into<String>,
        storage: Storage,
    ) -> Result<Option<Vec<u8>>, ApplicationError> {
        (self.read)(resource.into(), storage)
            .await
            .map_err(|_| ApplicationError::StoragePermissions)
    }
    /// `write_mtime` and `event_mtime` only need to be actual values if `storage` is
    /// [`Storage::Public`].
    pub async fn write(
        &self,
        resource: impl Into<String>,
        storage: WriteStorage,
        data: impl Into<Vec<u8>>,
        flush: bool,
    ) -> Result<(), ApplicationError> {
        match {
            let mut lock = self.file_cache.lock().unwrap();
            let v = lock.write(resource, data, storage, flush);
            drop(lock);
            v
        } {
            Ok(v) => v,
            Err((res, data)) => self._write(res, storage, data).await,
        }
    }
    async fn _write(
        &self,
        resource: impl Into<String>,
        storage: WriteStorage,
        data: impl Into<Vec<u8>>,
    ) -> Result<(), ApplicationError> {
        (self.write)(resource.into(), storage, data.into())
            .await
            .map_err(|_| ApplicationError::StoragePermissions)
    }
    pub async fn delete(
        &self,
        resource: impl Into<String> + AsRef<str>,
        storage: Storage,
    ) -> Result<(), ApplicationError> {
        match {
            let mut lock = self.file_cache.lock().unwrap();
            let v = lock.delete(resource, storage);
            drop(lock);
            v
        } {
            Ok(v) => v,
            Err(res) => self._delete(res, storage).await,
        }
    }
    async fn _delete(
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
    /// Sync the metadata to the disk.
    ///
    /// Please take care and not keep the locks of [`Self::metadata`] or [`Self::metadata_offline`]
    /// when calling this.
    ///
    /// Mappings:
    /// - [`Storage::Public`] syncs [`Self::metadata`]
    /// - [`Storage::Current`] syncs [`Self::metadata_offline`]
    /// - [`Storage::Meta`] returns `Ok(())` without doing anything.
    pub async fn sync_metadata(&self, storage: Storage) -> Result<(), ApplicationError> {
        (self.sync_metadata)(storage)
            .await
            .map_err(|()| ApplicationError::StoragePermissions)
    }

    pub async fn read_clean(&self) -> Result<Option<Vec<u8>>, ApplicationError> {
        self.read("clean", Storage::Meta).await
    }
    pub async fn write_clean(
        &self,
        data: impl Into<Vec<u8>>,
        flush: bool,
    ) -> Result<(), ApplicationError> {
        self.write("clean", WriteStorage::Meta, data, flush).await
    }

    pub async fn flush(&self) -> Result<(), ApplicationError> {
        info!("Flushing.");
        let (mut public, mut meta) = {
            let mut lock = self.file_cache.lock().unwrap();
            (
                core::mem::take(&mut lock.public),
                core::mem::take(&mut lock.meta),
            )
        };

        for (resource, (data, changed)) in &mut public {
            let old_changed = *changed;
            *changed = false;
            if !old_changed {continue;}
            if let Some((vec, mtime, event_mtime)) = data {
                self._write(
                    resource,
                    WriteStorage::Public(*mtime, *event_mtime),
                    vec.clone(),
                )
                .await?;
            } else {
                self._delete(resource, Storage::Public).await?;
            }
        }
        for (resource, (data, changed)) in &mut meta {
            let old_changed = *changed;
            *changed = false;
            if !old_changed {continue;}
            if let Some(vec) = data {
                self._write(resource, WriteStorage::Meta, vec.clone())
                    .await?;
            } else {
                self._delete(resource, Storage::Meta).await?;
            }
        }

        {
            let mut lock = self.file_cache.lock().unwrap();
            {
                public.extend(lock.public.drain());
                meta.extend(lock.meta.drain());
                lock.public = public;
                lock.meta = meta;
            }
        }
        Ok(())
    }
    /// Also clears the caches.
    pub async fn flush_out(&self) -> Result<(), ApplicationError> {
        info!("Flushing.");
        let (mut public, mut meta) = {
            let mut lock = self.file_cache.lock().unwrap();
            (
                core::mem::take(&mut lock.public),
                core::mem::take(&mut lock.meta),
            )
        };

        for (resource, (data, _)) in public.drain() {
            if let Some((vec, mtime, event_mtime)) = data {
                self._write(resource, WriteStorage::Public(mtime, event_mtime), vec)
                    .await?;
            } else {
                self._delete(resource, Storage::Public).await?;
            }
        }
        for (resource, (data, _)) in meta.drain() {
            if let Some(vec) = data {
                self._write(resource, WriteStorage::Meta, vec).await?;
            } else {
                self._delete(resource, Storage::Meta).await?;
            }
        }

        {
            let mut lock = self.file_cache.lock().unwrap();
            {
                public.extend(lock.public.drain());
                meta.extend(lock.meta.drain());
                lock.public = public;
                lock.meta = meta;
            }
        }
        Ok(())
    }
}

struct FileCache {
    // data the same as [`WriteStorage::Public`]
    public: HashMap<String, (Option<(Vec<u8>, WriteMtime, SystemTime)>, bool)>,
    meta: HashMap<String, (Option<Vec<u8>>, bool)>,
    max_size: usize,
}
impl FileCache {
    fn new(max_size: usize) -> Self {
        Self {
            public: HashMap::new(),
            meta: HashMap::new(),
            max_size,
        }
    }
    fn write(
        &mut self,
        resource: impl Into<String>,
        data: impl Into<Vec<u8>>,
        storage: WriteStorage,
        flush: bool,
    ) -> Result<Result<(), ApplicationError>, (String, Vec<u8>)> {
        let resource = resource.into();
        let data = data.into();
        match storage {
            WriteStorage::Public(mtime, event_mtime) => {
                if data.len() < self.max_size && !flush {
                    let tuple = (data, mtime, event_mtime);
                    if let Some(previous) = self.public.get(&resource) {
                        if previous.0.as_ref() == Some(&tuple) {
                            return Ok(Ok(()));
                        }
                    }
                    self.public.insert(resource, (Some(tuple), true));
                    return Ok(Ok(()));
                } else {
                    // if previous instance of this resource in cache, remove it.
                    self.public.remove(&resource);
                }
            }
            WriteStorage::Meta => {
                if data.len() < self.max_size && !flush {
                    if let Some(previous) = self.meta.get(&resource) {
                        if previous.0.as_ref() == Some(&data) {
                            println!("Skipping, changed: {}", previous.1);
                            return Ok(Ok(()));
                        }
                    }
                    self.meta.insert(resource, (Some(data), true));
                    return Ok(Ok(()));
                } else {
                    // if previous instance of this resource in cache, remove it.
                    self.meta.remove(&resource);
                }
            }
            WriteStorage::Current(_) => {}
        }

        Err((resource, data))
    }
    fn read_owned(
        &mut self,
        resource: impl Into<String> + AsRef<str>,
        storage: Storage,
    ) -> Result<Result<Option<Vec<u8>>, ApplicationError>, String> {
        self.read(resource, storage)
            .map(|r| r.map(|o| o.map(|v| v.to_vec())))
    }
    fn read(
        &mut self,
        resource: impl Into<String> + AsRef<str>,
        storage: Storage,
    ) -> Result<Result<Option<&'_ [u8]>, ApplicationError>, String> {
        match storage {
            Storage::Public => {
                if let Some(v) = self.public.get(resource.as_ref()) {
                    return Ok(Ok(v.0.as_ref().map(|(v, _, _)| v.as_slice())));
                }
            }
            Storage::Current => {}
            Storage::Meta => {
                if let Some(v) = self.meta.get(resource.as_ref()) {
                    return Ok(Ok(v.0.as_deref()));
                }
            }
        }
        Err(resource.into())
    }
    fn delete(
        &mut self,
        resource: impl Into<String> + AsRef<str>,
        storage: Storage,
    ) -> Result<Result<(), ApplicationError>, String> {
        match storage {
            Storage::Public => {
                if let Some(v) = self.public.get_mut(resource.as_ref()) {
                    v.0 = None;
                } else {
                    self.public.insert(resource.into(), (None, true));
                }
                Ok(Ok(()))
            }
            Storage::Current => Err(resource.into()),
            Storage::Meta => {
                if let Some(v) = self.meta.get_mut(resource.as_ref()) {
                    v.0 = None;
                } else {
                    self.meta.insert(resource.into(), (None, true));
                }
                Ok(Ok(()))
            }
        }
    }
}

#[derive(Clone)]
pub struct StateHandle {
    pub manager: Arc<Mutex<Manager>>,
    pub options: Arc<Options>,
    pub write: Arc<Mutex<WriteHalf>>,
    pub read: Arc<Mutex<ReadHalf>>,
    changed: Arc<Mutex<HashSet<String>>>,
}
impl StateHandle {
    pub async fn commit_and_send(&self) -> Result<(), ApplicationError> {
        commit_and_send(&self.manager, &self.options, &self.write, &self.changed).await
    }
}
pub struct Handle {
    inner: StateHandle,
    waiter: futures::channel::oneshot::Receiver<Result<(), ApplicationError>>,
}
impl Handle {
    pub fn state(&self) -> &StateHandle {
        &self.inner
    }
    pub async fn wait(self) -> Result<(), ApplicationError> {
        self.waiter.await.expect("agde panicked")
    }
}

pub async fn run(
    url: &str,
    mut manager: Manager,
    options: Arc<Options>,
) -> Result<Handle, DynError> {
    let state = options.read_clean().await?;

    if state.as_deref() != Some(b"y") {
        error!("State isn't clean.");

        let changes = options.diff().await?;
        if options.force_pull {
            if !changes.is_empty() {
                error!("Overriding local changes since no state file was found.");
            }

            for change in changes {
                let resource = match change {
                    MetadataChange::Modify(resource, _, _) | MetadataChange::Delete(resource) => {
                        resource
                    }
                };
                let actual = options.read(&resource, Storage::Public).await?;
                if let Some(actual) = actual {
                    options
                        .write(resource, WriteStorage::current(), actual, true)
                        .await?;
                }
            }

            options.write_clean("y", false).await?;
        } else if changes.is_empty() {
            options.write_clean("y", false).await?;
        }
    }

    let (mut write, mut read) = native::connect_ws(url).await?;

    // changes since last Storage sync
    let changed = Arc::new(Mutex::new(HashSet::new()));

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
        let mgr = Arc::clone(&manager);
        let write = Arc::clone(&write);
        let read = Arc::clone(&read);
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

                        handle_message(message, &mgr, &options, &write, &changed).await?;
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

    // trigger fast forward
    {
        let mut manager = manager.lock().await;
        if let Some(msg) = manager.process_fast_forward() {
            let mut write = write.lock().await;
            write.send(msg.to_bin().into()).await?;
        }
    }

    let flush_handle = {
        let options = Arc::clone(&options);
        tokio::spawn(async move {
            let mut i = 0;
            loop {
                let r = if i >= 60 {
                    options.flush_out().await
                } else {
                    options.flush().await
                };
                if let Err(err) = r {
                    error!("Error while flushing data: {err:?}");
                };
                tokio::time::sleep(options.flush_interval).await;
                i += 1;
            }
        })
    };

    // `TODO`: periodic calls (`clean_event_uuid_log_checks`, periodic (hash, event log) checks)
    let (oneshot_sender, oneshot_receiver) = futures::channel::oneshot::channel();

    tokio::spawn(async move {
        let (result, other) = futures::future::select(accept_handle, local_watcher)
            .await
            .into_inner();

        flush_handle.abort();
        other.abort();
        let result = result.expect("task panicked");

        let _ = oneshot_sender.send(result);
    });

    Ok(Handle {
        inner: StateHandle {
            manager,
            options,
            write,
            read,
            changed,
        },
        waiter: oneshot_receiver,
    })
}

async fn handle_message(
    message: agde::Message,
    mgr: &Mutex<Manager>,
    options: &Options,
    write: &Mutex<WriteHalf>,
    changed: &Mutex<HashSet<String>>,
) -> Result<(), ApplicationError> {
    let mut manager = mgr.lock().await;
    match message.recipient() {
        agde::Recipient::All => {}
        agde::Recipient::Selected(recipient) => {
            if recipient.uuid() != manager.uuid() {
                return Ok(());
            }
        }
    }
    let sender = message.sender();
    let message_uuid = message.uuid();
    match message.into_inner() {
        agde::MessageKind::Hello(hello) => {
            info!("Pier {} joined the network.", hello.uuid());
            let msg = manager.apply_hello(&hello);
            let mut w = write.lock().await;
            if w.send(msg.to_bin().into()).await.is_err() {
                return Err(ApplicationError::StreamBroken);
            }
        }
        agde::MessageKind::Welcome { info, recipient } => {
            if recipient.map_or(true, |intended| intended == manager.uuid()) {
                manager.apply_welcome(info);
            }
        }
        // ignore initial messages once connected.
        agde::MessageKind::InvalidUuid(_) | agde::MessageKind::MismatchingVersions(_) => {}
        agde::MessageKind::Event(event) => {
            info!(
                "Got event from pier {}: {:?} {event:?}",
                sender,
                SystemTime::now()
            );
            {
                changed.lock().await.insert(event.resource().to_owned());
            }

            if !sanitize(&event) {
                warn!("Received malicious event: {event:?}");
                return Ok(());
            }
            match manager.apply_event(&event, message_uuid) {
                Ok(mut applier) => {
                    let resource = applier.resource();

                    if let Some(resource) = resource {
                        // write to `.agde/clean` that we aren't clean (we have
                        // public diffs not applied to `current`)
                        options.write_clean("n", true).await?;

                        match applier.event().inner() {
                            agde::EventKind::Modify(_) => {
                                let resource_data = options.read(resource, Storage::Public).await?;

                                if let Some(data) = resource_data {
                                    info!("Read {:?} from public", String::from_utf8_lossy(&data));

                                    let resource = resource.to_owned();
                                    let data =
                                        applier.apply(data).map_or_else(|(_e, v)| v, identity);
                                    options
                                        .write(
                                            resource,
                                            WriteStorage::Public(WriteMtime::No, event.timestamp()),
                                            data,
                                            false,
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
                                        WriteStorage::Public(WriteMtime::No, event.timestamp()),
                                        Vec::new(),
                                        false,
                                    )
                                    .await?;
                            }
                            agde::EventKind::Delete(_) => {
                                options.delete(resource, Storage::Public).await?;
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
            let msg = manager.process_fast_forward_response(meta.clone(), sender);
            send(write, msg).await?;
        }
        agde::MessageKind::FastForwardReply(ff) => {
            println!("Fast forward changes");
            let changes = {
                let metadata = options.metadata().lock().await;
                metadata.changes(ff.metadata(), false)
            };
            options.sync_metadata(Storage::Public).await?;
            let mut sync_request = match manager.apply_fast_forward_reply(ff, sender) {
                Ok(v) => v,
                Err(agde::fast_forward::Error::UnexpectedPier) => return Ok(()),
                e => e.expect("internal state was unexpected. Bug in agde."),
            };
            println!("Changes: {changes:?}");
            for change in &changes {
                match change {
                    MetadataChange::Modify(res, _created, _) => {
                        let data = options
                            .read(res, Storage::Public)
                            .await?
                            .unwrap_or_default();
                        let mut sig = agde::den::Signature::new(128);
                        sig.write(&data);
                        let sig = sig.finish();
                        sync_request.insert(res.clone(), sig);
                    }
                    MetadataChange::Delete(res) => {
                        options.delete(res, Storage::Public).await?;
                    }
                }
            }

            let sync_request = sync_request.finish();
            let msg = manager.process_sync(sync_request, Some(changes));
            send(write, msg).await?;
        }
        agde::MessageKind::Sync(sync) => {
            let mut builder = manager.apply_sync(&sync, sender);
            while let Some((resource, signature)) = builder.next_signature() {
                println!("Signature from {resource}: {signature:?}");
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
            send(write, msg).await?;
        }
        agde::MessageKind::SyncReply(mut sync) => {
            println!("Sync reply: {:?}", sync);
            let mut changes = changed.lock().await;

            let (mut rewinder, metadata_applier) =
                if let Ok(rewinder) = manager.apply_sync_reply(&mut sync) {
                    rewinder
                } else {
                    warn!("Got erroneous sync message while fast-forwarding: {sync:?}");
                    return Ok(());
                };

            let mut error = false;
            for (resource, diff) in sync.diff() {
                let resource = resource.as_ref();
                let public = async {
                    let mut data = options
                        .read(resource, Storage::Public)
                        .await?
                        .unwrap_or_default();
                    println!("Read {:?}", std::str::from_utf8(&data));

                    let result = if diff.apply_overlaps(data.len()) {
                        let mut other = Vec::with_capacity(data.len() + 64);
                        let r = diff.apply(&data, &mut other);
                        data = other;
                        r
                    } else {
                        diff.apply_in_place(&mut data)
                    };
                    if result.is_err() {
                        return Err(ApplicationError::PiersRejected);
                    }
                    println!("applied {:?}", std::str::from_utf8(&data));
                    data = rewinder
                        .rewind(resource, data)
                        .map_or_else(|e| e.into_data(), identity);
                    println!("rewound {:?}", std::str::from_utf8(&data));
                    options
                        .write(
                            resource,
                            WriteStorage::Public(
                                WriteMtime::No,
                                rewinder.last_change_to_resource(resource),
                            ),
                            data,
                            false,
                        )
                        .await?;
                    Ok(())
                };
                let current = async {
                    let mut data = options
                        .read(resource, Storage::Current)
                        .await?
                        .unwrap_or_default();

                    let result = if diff.apply_overlaps_adaptive_end(data.len()) {
                        let mut other = Vec::with_capacity(data.len() + 64);
                        let r = diff.apply_adaptive_end(&data, &mut other);
                        data = other;
                        r
                    } else {
                        diff.apply_in_place_adaptive_end(&mut data)
                    };
                    if result.is_err() {
                        return Err(ApplicationError::PiersRejected);
                    }
                    options
                        .write(resource, WriteStorage::current(), data, true)
                        .await?;
                    Ok(())
                };
                match futures::future::try_join(public, current).await {
                    Err(ApplicationError::PiersRejected) => {
                        error = true;
                        break;
                    }
                    r => r?,
                };
            }
            if error {
                if manager.is_fast_forwarding() {
                    warn!(
                        "Fast forward failed due to an error with applying \
                        the sync response. Trying again."
                    );
                    if let Some(ff) = manager.process_fast_forward() {
                        send(write, ff).await?;
                    }
                    return Ok(());
                } else {
                    error!("Unhandled sync error!");
                    return Ok(());
                }
            }
            for (resource, _) in sync.diff() {
                {
                    changes.insert(resource.as_ref().to_owned());
                }
            }

            for resource in sync.delete() {
                let resource = resource.as_ref();
                {
                    changes.insert(resource.to_owned());
                }
                options.delete(resource, Storage::Public).await?;
            }

            if let Some(metadata_applier) = metadata_applier {
                // apply the remote changes (as we will be syncing them)
                //
                // this has the benefit of saving the metadata even if our data
                // is the same. If the data is the same, the pier will see that
                // the diff is empty and not send it back. Our public buffer
                // doesn't get modified, and next time we connect, we create a
                // signature again. That's bad. So by adjusting our metadata
                // here, we assue this is the first and only time we want to
                // get this version of the resource.
                //
                // `TODO`: send a touch array back with the sync response
                // instead, with the event_mtimes of the pier.
                // Then, we can remove `sync_metadata`.
                {
                    let mut metadata = options.metadata().lock().await;
                    println!("Metadata pre applied: {metadata:?}");
                    metadata_applier.apply(&mut metadata);
                    println!("Metadata after applied: {metadata:?}");
                    println!("ff: {:?}", metadata_applier.metadata());
                }
                options.sync_metadata(Storage::Public).await?;
            }

            drop(manager);
            drop(changes);
            commit_and_send(mgr, options, write, changed).await?;
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
        agde::MessageKind::Disconnect => {
            info!("Pier {sender} disconnected.");
            manager.apply_disconnect(sender);
        }
    }
    Ok(())
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

        for diff in &changes {
            let modern =
                manager.modern_resource_name(diff.resource(), manager.last_commit_or_epoch());
            // It's not worth sending updates when the resource has been deleted.
            if modern.is_some() {
                let event = match diff {
                    // `TODO`: Give successor
                    MetadataChange::Delete(res) => agde::Event::new(
                        agde::event::Kind::Delete(agde::event::Delete::new(res.to_owned(), None)),
                        &*manager,
                    ),
                    MetadataChange::Modify(resource, created, _) => {
                        let create_ev = if *created {
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

                        let mut current = if let Some(data) =
                            options.read(resource, Storage::Current).await?
                        {
                            data
                        } else {
                            warn!("Options::diff said current storage was modified, but it doesn't exist. Please check your `rough_resource_diff function`");
                            continue;
                        };

                        let public = options
                            .read(resource, Storage::Public)
                            .await?
                            .unwrap_or_default();

                        info!("Read {:?} from public", String::from_utf8_lossy(&public));
                        info!(
                            "Last check: {:?}, now {:?}",
                            manager.last_commit_or_epoch(),
                            SystemTime::now()
                        );

                        current = if let Some(current) =
                            rewind_current(&mut manager, *created, resource, &public, &current)
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

                        let event = if options.verify_diffs {
                            agde::event::Modify::new_with_verification(
                                resource.to_owned(),
                                &current,
                                &public,
                            )
                        } else {
                            agde::event::Modify::new(resource.to_owned(), &current, &public)
                        };
                        if !event.diff().is_empty() {
                            let event =
                                agde::Event::new(agde::event::Kind::Modify(event), &manager);

                            if let Some(ev) = create_ev {
                                messages.push(manager.process_event(ev).expect(
                                    "internal agde state bug - we're trying \
                                    to send an event while fast forwarding!",
                                ));
                            }
                            event
                        } else {
                            if let Some(ev) = create_ev {
                                messages.push(manager.process_event(ev).expect(
                                    "internal agde state bug - we're trying \
                                    to send an event while fast forwarding!",
                                ));
                            }
                            continue;
                        }
                    }
                };

                messages.push(manager.process_event(event).expect(
                    "internal agde state bug - we're trying \
                    to send an event while fast forwarding!",
                ))
            } else {
                // edited resource has been removed.
            }
        }

        manager.update_last_commit();
    }

    {
        {
            // this needed to be used as sometimes when a file has changed mtime but not been
            // modified, the diff doesn't get sent and we don't write to the file again.
            // This is here to make sure the metadata is updated with the latest mtimes.
            let mut offline_metadata = options.metadata_offline().lock().await;
            let mut metadata = options.metadata().lock().await;
            offline_metadata.apply_current_mtime_changes(&changes);
            // we also update metadata, as `offline_metadata` is set to `metadata` after each
            // commit.
            metadata.apply_current_mtime_changes(&changes);
        }
        futures::future::try_join(
            options.sync_metadata(Storage::Public),
            options.sync_metadata(Storage::Current),
        )
        .await?;
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

                    resource_data = applier
                        .apply(resource_data)
                        .map_or_else(|(_e, v)| v, identity);

                    options
                        .write(
                            resource,
                            WriteStorage::Public(WriteMtime::LookUpCurrent, event.timestamp()),
                            resource_data,
                            false,
                        )
                        .await?;
                }
                agde::EventKind::Create(_) => {
                    options
                        .write(
                            resource,
                            WriteStorage::Public(WriteMtime::LookUpCurrent, event.timestamp()),
                            "",
                            false,
                        )
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
                        .write(resource, WriteStorage::current(), actual, true)
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
        options.write_clean("y", false).await?;
    }
    Ok(())
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
        let unwound_public = unwinder.unwind(public, resource).ok()?;

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
        Err(agde::event::RewindError::Apply(_err, vec)) => Some(vec),
        Ok(vec) => Some(vec),
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
/// Sanitizes `ev`, treating it as a file system resource.
/// Returns `true` if allowed.
pub fn sanitize<S: agde::den::ExtendVec>(ev: &agde::Event<S>) -> bool {
    let resource = ev.resource();
    let path = Path::new(resource);
    path.is_relative() && !resource.contains("../")
}
