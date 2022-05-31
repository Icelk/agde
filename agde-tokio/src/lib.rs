use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap, HashSet};
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
use tokio::sync::{Mutex, MutexGuard};
use tokio_tungstenite::tungstenite;

pub mod native;
pub mod periodic;

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
    pub fn to_storage(self) -> Storage {
        match self {
            WriteStorage::Public(_, _) => Storage::Public,
            WriteStorage::Current(_) => Storage::Current,
            WriteStorage::Meta => Storage::Meta,
        }
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
pub type WriteFn = Box<dyn Fn(String, WriteStorage, Arc<Vec<u8>>) -> WriteFuture + Send + Sync>;
pub type DeleteFn = Box<dyn Fn(String, Storage) -> DeleteFuture + Send + Sync>;
pub type DiffFn = Box<dyn Fn() -> DiffFuture + Send + Sync>;
#[must_use]
pub struct Options {
    read: ReadFn,
    write: WriteFn,
    delete: DeleteFn,
    /// Returns a list of the resources which might have changed.
    rough_resource_diff: DiffFn,

    metadata: Arc<Mutex<Metadata>>,
    offline_metadata: Arc<Mutex<Metadata>>,
    file_cache: Arc<Mutex<FileCache>>,

    /// For how long to wait for welcomes.
    startup_timeout: Duration,
    sync_interval: Duration,
    periodic_interval: Duration,

    force_pull: bool,
    /// Verifies outgoing Modify events to be correct.
    /// A bit of a performance hit, but generally recommended.
    verify_diffs: bool,
    disable_public_storage: bool,
}
impl Options {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        read: ReadFn,
        write: WriteFn,
        delete: DeleteFn,
        rough_resource_diff: DiffFn,
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
            metadata,
            offline_metadata,
            file_cache: Arc::new(Mutex::new(FileCache::new(1024 * 8))),
            startup_timeout,
            sync_interval,
            periodic_interval: Duration::from_secs(30),
            force_pull,
            verify_diffs,
            disable_public_storage: false,
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
        self.periodic_interval = flush_interval;
        self
    }
    pub async fn with_file_cache_max_size(self, max_size: usize) -> Self {
        self.file_cache.lock().await.max_size = max_size;
        self
    }
    pub fn with_no_public_storage(mut self) -> Self {
        self.disable_public_storage = true;
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
    #[must_use]
    pub fn public_storage_disabled(&self) -> bool {
        self.disable_public_storage
    }

    /// The metadata of both the public and current storage.
    /// Calling [`Metadata::changes`] on `this.changes(offline_metadata)`
    /// gets you the changes to get current storage the same as the public.
    ///
    /// Can be thought as the metadata of the `remote` in git.
    pub fn metadata(&self) -> &Mutex<Metadata> {
        &self.metadata
    }
    /// The metadata of the [`Storage::Current`].
    ///
    /// Can be thought as the metadata of our local state in git,
    /// that has not yet been committed (if it's not clean).
    pub fn metadata_offline(&self) -> &Mutex<Metadata> {
        &self.offline_metadata
    }
}
impl Options {
    pub async fn read(
        &self,
        resource: impl Into<String> + AsRef<str>,
        mut storage: Storage,
    ) -> Result<Option<Arc<Vec<u8>>>, ApplicationError> {
        info!("Cached read from {:?} in {storage:?}", resource.as_ref());
        if self.disable_public_storage && storage == Storage::Current {
            storage = Storage::Public;
        }
        match {
            let mut lock = self.file_cache.lock().await;
            let v = lock.read(resource, storage);
            drop(lock);
            v
        } {
            Ok(v) => v,
            Err(res) => {
                let file = self._read(res.clone(), storage).await?;
                let file = file.map(Arc::new);
                {
                    let mut lock = self.file_cache.lock().await;
                    if let Some(file) = file.as_ref() {
                        // this data will never be written!
                        let storage = match storage {
                            Storage::Public => {
                                WriteStorage::Public(WriteMtime::No, SystemTime::UNIX_EPOCH)
                            }
                            Storage::Current => WriteStorage::Current(WriteMtime::No),
                            Storage::Meta => WriteStorage::Meta,
                        };
                        let _ = lock.write(res, Arc::clone(file), storage, false, true);
                    } else {
                        let _ = lock.delete(res, storage);
                    }
                }
                Ok(file)
            }
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
        resource: impl Into<String> + AsRef<str>,
        storage: WriteStorage,
        data: Arc<Vec<u8>>,
        flush: bool,
    ) -> Result<(), ApplicationError> {
        info!(
            "Cached write to {:?} in {:?}",
            resource.as_ref(),
            storage.to_storage()
        );
        if self.disable_public_storage && matches!(storage, WriteStorage::Current(_)) {
            return Ok(());
        }
        match {
            let mut lock = self.file_cache.lock().await;
            let v = lock.write(resource.into(), data, storage, flush, false);
            drop(lock);
            v
        } {
            Ok(()) => Ok(()),
            Err((res, data)) => self._write(res, storage, data).await,
        }
    }
    async fn _write(
        &self,
        resource: impl Into<String>,
        storage: WriteStorage,
        data: Arc<Vec<u8>>,
    ) -> Result<(), ApplicationError> {
        (self.write)(resource.into(), storage, data)
            .await
            .map_err(|_| ApplicationError::StoragePermissions)
    }
    pub async fn delete(
        &self,
        resource: impl Into<String> + AsRef<str>,
        storage: Storage,
    ) -> Result<(), ApplicationError> {
        info!("Cached delete to {:?} in {storage:?}", resource.as_ref());
        if self.disable_public_storage && storage == Storage::Current {
            return Ok(());
        }
        match {
            let mut lock = self.file_cache.lock().await;
            let v = lock.delete(resource, storage);
            drop(lock);
            v
        } {
            Ok(()) => Ok(()),
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
        if self.disable_public_storage {
            return Ok(vec![]);
        }

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
        match storage {
            Storage::Public => {
                let lock = self.metadata().lock().await;
                let data = bincode::serde::encode_to_vec(
                    &*lock,
                    bincode::config::standard().write_fixed_array_length(),
                )
                .expect("failed to serialize metadata");
                self.write(
                    "metadata".to_owned(),
                    WriteStorage::Meta,
                    Arc::new(data),
                    true,
                )
                .await?;
            }
            Storage::Current => {
                let lock = self.metadata_offline().lock().await;
                let data = bincode::serde::encode_to_vec(
                    &*lock,
                    bincode::config::standard().write_fixed_array_length(),
                )
                .expect("failed to serialize metadata");
                self.write(
                    "metadata-offline".to_owned(),
                    WriteStorage::Meta,
                    Arc::new(data),
                    true,
                )
                .await?;
            }
            Storage::Meta => {}
        }
        Ok(())
    }

    pub async fn read_clean(&self) -> Result<Option<Arc<Vec<u8>>>, ApplicationError> {
        self.read("clean", Storage::Meta).await
    }
    pub async fn write_clean(
        &self,
        data: impl Into<Vec<u8>>,
        flush: bool,
    ) -> Result<(), ApplicationError> {
        // if we disabled public storage, we don't care about the state of merging with public!
        if self.disable_public_storage {
            return Ok(());
        }
        self.write("clean", WriteStorage::Meta, Arc::new(data.into()), flush)
            .await
    }

    pub async fn flush(&self) -> Result<(), ApplicationError> {
        info!("Flushing.");
        let (public, meta) = {
            let mut lock = self.file_cache.lock().await;
            for (_, status) in &mut lock.public.values_mut() {
                *status = FileStatus::Flushed;
            }
            for (_, status) in &mut lock.meta.values_mut() {
                *status = FileStatus::Flushed;
            }

            (lock.public.clone(), lock.meta.clone())
        };

        let public_iter = public.iter().map(|(resource, (data, status))| async move {
            if *status != FileStatus::Cached {
                return Ok(());
            }
            if let Some(PublicFile(vec, mtime, event_mtime)) = data {
                self._write(
                    resource,
                    WriteStorage::Public(*mtime, *event_mtime),
                    vec.clone(),
                )
                .await?;
            } else {
                self._delete(resource, Storage::Public).await?;
            }
            Ok(())
        });
        let meta_iter = meta.iter().map(|(resource, (data, status))| async move {
            if *status != FileStatus::Cached {
                return Ok(());
            }
            if let Some(vec) = data {
                self._write(resource, WriteStorage::Meta, vec.clone())
                    .await?;
            } else {
                self._delete(resource, Storage::Meta).await?;
            }
            Ok(())
        });
        let public_future = futures::future::try_join_all(public_iter);
        let meta_future = futures::future::try_join_all(meta_iter);
        // batch them up in 1 job to complete IO in one go
        futures::future::try_join(public_future, meta_future).await?;

        Ok(())
    }
    /// Also clears the caches.
    pub async fn flush_out(&self) -> Result<(), ApplicationError> {
        info!("Flushing out cache.");
        // keep the lock so nobody else reads while we are flushing.
        let mut lock = self.file_cache.lock().await;
        // clear the caches

        for (resource, (data, status)) in lock.public.drain() {
            if status == FileStatus::Read {
                continue;
            }
            if let Some(PublicFile(vec, mtime, event_mtime)) = data {
                self._write(resource, WriteStorage::Public(mtime, event_mtime), vec)
                    .await?;
            } else {
                self._delete(resource, Storage::Public).await?;
            }
        }
        for (resource, (data, status)) in lock.meta.drain() {
            if status == FileStatus::Read {
                continue;
            }
            if let Some(vec) = data {
                self._write(resource, WriteStorage::Meta, vec).await?;
            } else {
                self._delete(resource, Storage::Meta).await?;
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone)]
struct PublicFile(Arc<Vec<u8>>, WriteMtime, SystemTime);
impl PartialEq for PublicFile {
    fn eq(&self, other: &Self) -> bool {
        *self.0 == *other.0 && self.1 == other.1 && self.2 == other.2
    }
}
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum FileStatus {
    Cached,
    Flushed,
    /// Read from the FS
    Read,
}
impl FileStatus {
    fn from_fs(from_fs: bool) -> Self {
        if from_fs {
            Self::Read
        } else {
            Self::Cached
        }
    }
}
struct FileCache {
    /// data the same as [`WriteStorage::Public`]
    ///
    /// The bool is whether or not this has changed since last flush.
    /// If the option is [`None`], the resource is not present (deleted).
    public: HashMap<String, (Option<PublicFile>, FileStatus)>,
    /// data the same as [`WriteStorage::Meta`]
    #[allow(clippy::type_complexity)]
    meta: HashMap<String, (Option<Arc<Vec<u8>>>, FileStatus)>,
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
    #[allow(clippy::type_complexity)] // internal API
    fn write(
        &mut self,
        resource: String,
        data: Arc<Vec<u8>>,
        storage: WriteStorage,
        flush: bool,
        from_fs: bool,
    ) -> Result<(), (String, Arc<Vec<u8>>)> {
        match storage {
            WriteStorage::Public(mtime, event_mtime) => {
                if data.len() < self.max_size && !flush {
                    let file = PublicFile(data, mtime, event_mtime);
                    if let Some(previous) = self.public.get(&resource) {
                        if previous.0.as_ref() == Some(&file) && previous.1 != FileStatus::Read {
                            return Ok(());
                        }
                    }
                    self.public
                        .insert(resource, (Some(file), FileStatus::from_fs(from_fs)));
                    return Ok(());
                } else {
                    // if previous instance of this resource in cache, remove it.
                    self.public.remove(&resource);
                }
            }
            WriteStorage::Meta => {
                if data.len() < self.max_size && !flush {
                    if let Some(previous) = self.meta.get(&resource) {
                        if previous.0.as_ref() == Some(&data) {
                            return Ok(());
                        }
                    }
                    self.meta
                        .insert(resource, (Some(data), FileStatus::from_fs(from_fs)));
                    return Ok(());
                } else {
                    // if previous instance of this resource in cache, remove it.
                    self.meta.remove(&resource);
                }
            }
            WriteStorage::Current(_) => {}
        }

        Err((resource, data))
    }
    #[allow(clippy::type_complexity)] // internal API
    fn read(
        &mut self,
        resource: impl Into<String> + AsRef<str>,
        storage: Storage,
    ) -> Result<Result<Option<Arc<Vec<u8>>>, ApplicationError>, String> {
        match storage {
            Storage::Public => {
                if let Some(v) = self.public.get(resource.as_ref()) {
                    return Ok(Ok(v.0.as_ref().map(|file| Arc::clone(&file.0))));
                }
            }
            Storage::Current => {}
            Storage::Meta => {
                if let Some(v) = self.meta.get(resource.as_ref()) {
                    return Ok(Ok(v.0.as_ref().map(Arc::clone)));
                }
            }
        }
        Err(resource.into())
    }
    /// # Errors
    ///
    /// Returns `resource` if `storage` is [`Storage::Current`].
    fn delete(
        &mut self,
        resource: impl Into<String> + AsRef<str>,
        storage: Storage,
    ) -> Result<(), String> {
        match storage {
            Storage::Public => {
                if let Some(v) = self.public.get_mut(resource.as_ref()) {
                    v.0 = None;
                } else {
                    self.public
                        .insert(resource.into(), (None, FileStatus::Cached));
                }
                Ok(())
            }
            Storage::Current => Err(resource.into()),
            Storage::Meta => {
                if let Some(v) = self.meta.get_mut(resource.as_ref()) {
                    v.0 = None;
                } else {
                    self.meta
                        .insert(resource.into(), (None, FileStatus::Cached));
                }
                Ok(())
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
    /// Commit and send our changes.
    /// This also pulls the changes from others to the `current` storage.
    ///
    /// The `cursors` are a list of byte-index positions in the resources where you have cursors
    /// (read: text-editing cursors). After calling this function, their [`Cursor::index`] are
    /// modified according to the changes from your piers. This means your user's cursor changes
    /// position when pulling changes, to the logically same place, even if edits happened above in
    /// the document.
    pub async fn commit_and_send(
        &self,
        cursors: &mut [Cursor<'_>],
    ) -> Result<(), ApplicationError> {
        commit_and_send(
            &self.manager,
            &self.options,
            &self.write,
            &self.changed,
            cursors,
        )
        .await
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

/// A cursor into a resource.
///
/// Used by [`StateHandle::commit_and_send`] to calculate where the user's cursor should move after
/// merging others' data.
#[derive(Debug)]
pub struct Cursor<'a> {
    pub resource: &'a str,
    pub index: usize,
}

pub async fn run(
    url: &str,
    mut manager: Manager,
    options: Arc<Options>,
) -> Result<Handle, DynError> {
    let state = options.read_clean().await?;

    // if we disabled public storage, we don't care about the state of merging with public!
    if state.map_or(false, |state| &**state != b"y") && !options.disable_public_storage {
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
        } else {
            return Err("state not clean".into());
        }
    }

    let (mut write, mut read) = native::connect_ws(url).await?;

    // changes since last Storage sync
    let changed = Arc::new(Mutex::new(HashSet::new()));

    {
        let message = { manager.process_hello() };
        write
            .send(to_compressed_bin(&message).into())
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
                            if let Ok(message) = from_compressed_bin(&data) {
                                total += 1;
                                match message.inner() {
                                    agde::MessageKind::Hello(hello) => {
                                        // copied from loop below.
                                        info!("Pier {} joined the network.", hello.uuid());
                                        let msg = manager.apply_hello(hello);
                                        if write.send(to_compressed_bin(&msg).into()).await.is_err()
                                        {
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
                        let message = from_compressed_bin(&data);
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

    // `TODO`: move this out of the run function, so people can pass the `cursors` to
    // `commit_and_send`.
    let local_watcher: tokio::task::JoinHandle<Result<(), ApplicationError>> = {
        let options = Arc::clone(&options);
        let manager = Arc::clone(&manager);
        let write = Arc::clone(&write);
        let changed = Arc::clone(&changed);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(options.sync_interval).await;
                // `TODO`: cursors, see above
                commit_and_send(&manager, &options, &write, &changed, &mut []).await?;
            }
        })
    };

    // trigger fast forward
    {
        let mut mgr = manager.lock().await;
        if let Some(msg) = mgr
            .process_fast_forward()
            .expect("BUG: Internal agde state error when trying to fast forward.")
        {
            drop(mgr);
            let manager = Arc::clone(&manager);
            let write2 = Arc::clone(&write);
            let pier = if let agde::Recipient::Selected(pier) = msg.recipient() {
                pier.uuid()
            } else {
                unreachable!("A fast forward message has to have a recipient")
            };
            tokio::spawn(async move {
                let mut pier = pier;
                loop {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    let mut manager = manager.lock().await;

                    // we're still fast forwarding
                    if manager.is_fast_forwarding() {
                        info!("Pier didn't respond to fast forward. Trying again.");
                        let action = manager.apply_cancelled(pier);

                        if let agde::CancelAction::FastForward = action {
                            let ff = manager.process_fast_forward().expect(
                                "BUG: Internal agde state error when trying \
                                to recover from a failed fast forward",
                            );

                            if let Some(ff) = ff {
                                drop(manager);

                                pier = if let agde::Recipient::Selected(pier) = ff.recipient() {
                                    pier.uuid()
                                } else {
                                    unreachable!("A fast forward message has to have a recipient")
                                };

                                if let Err(err) = send(&write2, &ff).await {
                                    error!("Error when trying to fast forward to other piers after one failed: {err:?}");
                                };
                                continue;
                            }
                        }
                    }
                    break;
                }
            });
            send(&write, &msg).await?;
        } else {
            drop(mgr);
            // cursors: we hopefully aren't reading this currently
            commit_and_send(&manager, &options, &write, &changed, &mut []).await?;
        }
    }
    info!("Began fast forwarding.");

    let periodic_handle = {
        let options = Arc::clone(&options);
        let mgr = Arc::clone(&manager);
        let write = Arc::clone(&write);

        tokio::spawn(async move {
            periodic::start(&options, &mgr, &write).await;
        })
    };

    let (oneshot_sender, oneshot_receiver) = futures::channel::oneshot::channel();

    tokio::spawn(async move {
        let (result, other) = futures::future::select(accept_handle, local_watcher)
            .await
            .into_inner();

        periodic_handle.abort();
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
    mgr: &Arc<Mutex<Manager>>,
    options: &Options,
    write: &Arc<Mutex<WriteHalf>>,
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
            drop(manager);
            if send(write, &msg).await.is_err() {
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
                                    let data = data.as_ref().clone();
                                    let resource = resource.to_owned();
                                    let data =
                                        applier.apply(data).map_or_else(|(_e, v)| v, identity);
                                    options
                                        .write(
                                            resource,
                                            WriteStorage::Public(WriteMtime::No, event.timestamp()),
                                            Arc::new(data),
                                            false,
                                        )
                                        .await?;
                                } else {
                                    periodic::event_log_check(mgr, write).await;
                                    warn!(
                                        "Got Modify event, but resource doesn't exist. \
                                        Reconnecting might help, but this could be an \
                                        extortion to attempt to make you disconnect."
                                    );
                                };
                            }
                            agde::EventKind::Create(_) => {
                                options
                                    .write(
                                        resource,
                                        WriteStorage::Public(WriteMtime::No, event.timestamp()),
                                        Arc::new(Vec::new()),
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
                Err(err) => match err {
                    agde::log::Error::EventInFuture => {
                        warn!("Pier {sender} send an event from the future. Running a log check.");
                        periodic::event_log_check(mgr, write).await;
                    }
                    // if in ff, then do nothing.
                    agde::log::Error::FastForwardInProgress => {}
                },
            };
        }
        agde::MessageKind::FastForward(_ff) => {
            info!("Helping {sender} fast forward.");
            let meta = options.metadata().lock().await;
            let msg = manager.process_fast_forward_response(meta.clone(), sender);
            drop(manager);
            send(write, &msg).await?;
        }
        agde::MessageKind::FastForwardReply(ff) => {
            let changes = {
                let metadata = options.metadata().lock().await;
                metadata.changes(ff.metadata(), false)
            };

            info!("The pier {} responded to our fast forward request.", sender);
            options.sync_metadata(Storage::Public).await?;
            let mut sync_request = match manager.apply_fast_forward_reply(ff, sender) {
                Ok(v) => v,
                Err(agde::fast_forward::Error::UnexpectedPier) => return Ok(()),
                e => e.expect("internal state was unexpected. Bug in agde."),
            };
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
            info!("We sent a sync request after fast forwarding.");
            let msg = manager.process_sync(sync_request, Some(changes));
            drop(manager);
            send(write, &msg).await?;
        }
        agde::MessageKind::Sync(sync) => {
            let mut builder = manager.apply_sync(&sync, sender);
            // `TODO`: do this in parallel, but that uses more memory! Requires a streaming API.
            while let Some((resource, signature, unwinder)) = builder.next_signature() {
                let data = options.read(resource, Storage::Public).await?;
                let data = data.as_deref().cloned();
                let mut data = if let Some(d) = data {
                    Some(d)
                } else {
                    continue;
                };
                // so we don't call builder ↓, as that would be multiple
                // mutable borrows.
                let resource = resource.to_owned();
                let data_len = data.as_ref().unwrap().len();
                if let Some(unwinder) = unwinder {
                    data = unwinder
                        .unwind(data.unwrap(), &resource)
                        .or_else(|e| e.ignore_apply_err(()))
                        .ok();
                }
                if let Some(data) = data {
                    let diff = signature.diff(&data);
                    builder.add_diff(resource, diff);
                } else {
                    // if the resource has since been destroyed, return something so they don't
                    // delete ut, but make sure the diff doesn't change anything (`empty`).
                    builder.add_diff(resource, agde::den::Difference::empty(data_len, 8));
                }
            }
            let response = builder.finish(&manager);
            let msg = manager.process_sync_reply(response);
            drop(manager);
            info!("We sent a sync response to {sender}.");
            send(write, &msg).await?;
        }
        agde::MessageKind::SyncReply(sync) => {
            info!("We received sync response from {sender}.");
            handle_sync_reply(manager, options, write, sync, sender).await?;
            // cursors: we hopefully aren't reading this currently.
            //  if we are, then this probably contains many Unknown segments.
            //  The user also shouldn't edit a resource at the same time as it gets synced.
            commit_and_send(mgr, options, write, changed, &mut []).await?;
        }
        agde::MessageKind::HashCheck(hc) => {
            info!("Got hash check request from {sender}.");
            let mut builder = manager.apply_hash_check(hc, sender);
            {
                let metadata = options.metadata().lock().await;
                // `TODO`: do this in parallel, but that uses more memory! Requires ↓
                for (resource, _meta) in metadata.iter() {
                    if builder.matches(resource) {
                        // `TODO`: create a streaming API for `options` which hashes the resource
                        // without reading the whole thing into memory.
                        let data = options.read(resource, Storage::Public).await?;
                        if let Some(data) = data {
                            let data = data.as_ref().clone();
                            // unwind the data
                            let unwinder = builder.unwinder();
                            let data = unwinder
                                .unwind(data, resource)
                                .or_else(|e| e.ignore_apply_err(()))
                                .ok();

                            // if resource was just created, ignore
                            if let Some(data) = data {
                                let mut hash = agde::hash_check::ResponseHasher::new();
                                hash.write(&data);
                                builder.insert(resource.to_owned(), hash.finish());
                            }
                        } else {
                            // do nothing, by just not adding to the response, we signal we don't
                            // have it when the pier later checks the differences.
                        }
                    }
                }
            }

            let response = builder.finish();
            let msg = manager.process_hash_check_reply(response);
            drop(manager);
            send(write, &msg).await?;
        }
        agde::MessageKind::HashCheckReply(hc) => {
            if manager.is_fast_forwarding() {
                warn!(
                    "Pier {sender} tried to send a hash check reply when we are fast forwarding."
                );
                return Ok(());
            }
            info!("Got hash check response from {sender}.");
            debug!("Hash check from {sender}: {:#?}", hc.hashes());
            debug!("Our metadata now: {:#?}", options.metadata().lock().await);

            let mut our_hashes = BTreeMap::new();

            {
                let metadata = options.metadata().lock().await;
                let mut unwinder = hc.unwinder(&manager);
                // `TODO`: do this in parallel, but that uses more memory! Requires ↓
                for (resource, _meta) in metadata.iter() {
                    if hc.matches(resource) {
                        // `TODO`: create a streaming API for `options` which hashes the resource
                        // without reading the whole thing into memory.
                        let data = options.read(resource, Storage::Public).await?;
                        if let Some(data) = data {
                            let data = data.as_ref().clone();
                            if data.len() < 200 {
                                debug!("Read {resource:?}: {:?}", std::str::from_utf8(&data));
                            }
                            // unwind the data
                            let unwinder = unwinder.unwinder();
                            let data = unwinder
                                .unwind(data, resource)
                                .or_else(|e| e.ignore_apply_err(()))
                                .ok();

                            // if resource was just created, ignore
                            if let Some(data) = data {
                                if data.len() < 200 {
                                    debug!(
                                        "Unwound {resource:?}: {:?}",
                                        std::str::from_utf8(&data)
                                    );
                                }

                                let mut hash = agde::hash_check::ResponseHasher::new();
                                hash.write(&data);
                                hash.write(&manager.uuid().inner().to_le_bytes());
                                our_hashes.insert(resource.to_owned(), hash.finish());
                            }
                        } else {
                            // do nothing, by just not adding to the response, we signal we don't
                            // have it when the pier later checks the differences.
                        }
                    }
                }
            }
            debug!("Our hashes: {:#?}", our_hashes);

            let (sync, delete) = manager
                .apply_hash_check_reply(&hc, sender, &our_hashes)
                .expect("Tried to execute a hash check while fast forwarding.");

            // free memory
            drop(our_hashes);

            if !delete.is_empty() {
                info!("Deleting resources after hash check: {delete:?}");
                debug!(
                    "Metadata when deleting: {:#?}",
                    options.metadata().lock().await
                );
            }

            // delete all in parallell
            futures::future::try_join_all(delete.into_iter().map(|r| {
                futures::future::try_join(
                    options.delete(r.clone(), Storage::Public),
                    options.delete(r, Storage::Current),
                )
            }))
            .await?;

            if let Some((mut sync, resources)) = sync {
                let mut unwinder = hc.unwinder(&manager);
                // `TODO`: do this in parallel, but that uses more memory! Requires a streaming API.
                for resource in resources {
                    let data = options.read(&resource, Storage::Public).await?;
                    if let Some(data) = data {
                        let data = data.as_ref().clone();
                        // unwind the data
                        let unwinder = unwinder.unwinder();
                        let data = match unwinder
                            .unwind(data, &resource)
                            .or_else(|e| e.ignore_apply_err(()))
                        {
                            Ok(data) => data,
                            // if `resource` was created since the beginning of the hash check's
                            // event log reach.
                            //
                            // If we don't add a signature here, the worst that will happen is that
                            // the responding pier will send the whole resource, as if we don't
                            // have it.
                            Err(()) => continue,
                        };

                        let mut sig = agde::den::Signature::new(128);
                        sig.write(&data);
                        let sig = sig.finish();
                        sync.insert(resource, sig);
                    } else {
                        // do nothing, by just not adding to the response, we signal we don't
                        // have it when the pier later checks the differences.
                    }
                }
                let msg = manager.process_sync(sync.finish(), None);
                info!("The hash check doesn't match. Send sync message.");
                drop(manager);
                send(write, &msg).await?;
            }
        }
        agde::MessageKind::LogCheck {
            conversation_uuid,
            check,
        } => {
            let action = manager.apply_event_uuid_log_check(check, conversation_uuid, sender);
            match action {
                agde::LogCheckAction::Send(ec) => {
                    let msg = manager.process_event_log_check_reply(ec, conversation_uuid);
                    drop(manager);
                    send(write, &msg).await?;
                    periodic::assure_event_log_check(
                        Arc::clone(mgr),
                        Arc::clone(write),
                        conversation_uuid,
                    )
                    .await;
                }
                agde::LogCheckAction::Nothing => {}
            }
        }
        agde::MessageKind::LogCheckReply {
            conversation_uuid,
            check,
        } => {
            manager.apply_log_check_reply(check, conversation_uuid, sender);
        }
        agde::MessageKind::Cancelled(_) => match manager.apply_cancelled(sender) {
            agde::CancelAction::Nothing => {}
            agde::CancelAction::HashCheck(pier) => {
                // UNWRAP: see the docs of `apply_cancelled`.
                let msg = manager
                    .process_hash_check(pier)
                    .expect("BUG: Internal agde state error when accepting cancel event.");
                drop(manager);
                send(write, &msg).await?;
            }
            agde::CancelAction::FastForward => {
                // UNWRAP: see the docs of `apply_cancelled`.
                let msg = manager
                    .process_fast_forward()
                    .expect("BUG: Internal agde state error when accepting cancel event.");
                drop(manager);
                if let Some(msg) = msg {
                    send(write, &msg).await?;
                }
            }
        },
        agde::MessageKind::Disconnect => {
            info!("Pier {sender} disconnected.");
            manager.apply_disconnect(sender);
        }
    }
    Ok(())
}
/// Just returns `Ok(())` if the manager is fast forwarding.
async fn commit_and_send(
    manager: &Mutex<Manager>,
    options: &Options,
    write: &Mutex<WriteHalf>,
    changed: &Mutex<HashSet<String>>,
    cursors: &mut [Cursor<'_>],
) -> Result<(), ApplicationError> {
    let mut manager = manager.lock().await;
    if manager.is_fast_forwarding() {
        info!("Don't commit and send - we're fast forwarding.");
        return Ok(());
    }

    let changes = options.diff().await?;

    info!("Got diffs {changes:?}");

    let mut messages = Vec::with_capacity(changes.len());

    {
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

                        let current =
                            if let Some(data) = options.read(resource, Storage::Current).await? {
                                data
                            } else {
                                warn!(
                                    "Options::diff said current storage was modified, \
                                    but it doesn't exist. Please check \
                                    your `rough_resource_diff function`"
                                );
                                continue;
                            };
                        let mut current = current.as_ref().clone();

                        let public = options
                            .read(resource, Storage::Public)
                            .await?
                            .unwrap_or_default();
                        let public = public.as_ref().clone();

                        info!(
                            "Last check: {:?}, now {:?}",
                            manager.last_commit_or_epoch(),
                            SystemTime::now()
                        );

                        current = if let Some(current) = rewind_current(
                            &mut manager,
                            *created,
                            resource,
                            public.as_slice(),
                            current,
                            cursors,
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

                debug!("Sending event: {event:?}");
                messages.push(manager.process_event(event).expect(
                    "BUG: internal agde state error - we're trying \
                    to send an event while fast forwarding!",
                ))
            } else {
                // edited resource has been removed.
            }
        }

        manager.update_last_commit();
    }

    if !changes.is_empty() {
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
    }

    // Execute `apply` and `send` at the same time!
    let apply = async {
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
                    let resource_data = options.read(&resource, Storage::Public).await?.expect(
                        "we trust our own data - there must \
                        have been a create event before modify",
                    );
                    let mut resource_data = resource_data.as_ref().clone();

                    resource_data = applier
                        .apply(resource_data)
                        .map_or_else(|(_e, v)| v, identity);

                    options
                        .write(
                            resource,
                            WriteStorage::Public(WriteMtime::LookUpCurrent, event.timestamp()),
                            Arc::new(resource_data),
                            false,
                        )
                        .await?;
                }
                agde::EventKind::Create(_) => {
                    options
                        .write(
                            resource,
                            WriteStorage::Public(WriteMtime::LookUpCurrent, event.timestamp()),
                            Arc::new(Vec::new()),
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
                let actual = options.read(resource, Storage::Public).await?;
                if let Some(actual) = actual {
                    options
                        .write(resource, WriteStorage::current(), actual, true)
                        .await?;
                } else {
                    info!(
                        "Deleting {resource:?} when moving to current, \
                        as the resource doesn't exist in public!"
                    );
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
            let message = to_compressed_bin(message);

            write
                .feed(message.into())
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
    public: impl Into<Vec<u8>>,
    current: impl Into<Vec<u8>>,
    cursors: &mut [Cursor<'_>],
) -> Option<Vec<u8>> {
    let current = current.into();
    let last_commit = manager.last_commit_or_epoch();
    let offsets = if !created {
        let mut unwinder = manager.unwinder_to(last_commit);

        // here, `resource` is just the modern name. See the code
        // above for more info
        let unwound_public = unwinder.unwind(public, resource).ok()?;

        let old_diff = agde::event::diff(&unwound_public, &current);

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

    let r = match rewinder.rewind_with_modify_diff(resource, current, |diff| {
        let mut diff = diff.clone();
        let original_data_len = diff.original_data_len();
        offsets.apply_single(&mut diff);
        // ignore `offsets`'s changes, as `apply_adaptive_end`
        // takes care of that.
        diff.set_original_data_len(original_data_len);
        Cow::Owned(diff)
    }) {
        Err(agde::event::RewindError::ResourceDestroyed(_)) => None,
        Err(agde::event::RewindError::Apply(err, vec)) => {
            warn!("Error when rewinding current storage (a stored diff is invalid): {err:?}");
            Some(vec)
        }
        Ok(vec) => Some(vec),
    };

    // the resource isn't destroyed
    if r.is_some() {
        let mut cursors = cursors
            .iter_mut()
            .filter(|c| {
                c.resource.strip_prefix("./").unwrap_or(c.resource)
                    == resource.strip_prefix("./").unwrap_or(resource)
            })
            .peekable();
        // we have at least 1 applicable cursor
        if cursors.peek().is_some() {
            let mut offsets = agde::utils::Offsets::new();
            for diff in rewinder
                .events()
                .filter(|ev| ev.resource() == resource)
                .filter_map(agde::Event::diff)
            {
                // len_diff doesn't for `transform_index`.
                offsets.add_diff(diff, 0)
            }
            for cursor in cursors {
                cursor.index = offsets.transform_index(cursor.index);
            }
        }
    }

    r
}

async fn handle_sync_reply(
    mut manager: MutexGuard<'_, Manager>,
    options: &Options,
    write: &Mutex<WriteHalf>,
    mut sync: agde::sync::Response,
    sender: agde::Uuid,
) -> Result<(), ApplicationError> {
    let action = if let Ok(action) = manager.apply_sync_reply(&mut sync, sender) {
        action
    } else {
        warn!("Got erroneous sync message while fast-forwarding: {sync:?}");
        return Ok(());
    };

    match action {
        agde::SyncReplyAction::FastForward {
            rewinder,
            metadata_applier,
        } => {
            let mut public_rewinder = rewinder;
            let mut current_rewinder = public_rewinder.clone();
            let mut error = false;
            // `TODO`: do this in parallel, but that uses more memory! Requires a streaming API.
            for (resource, diff) in sync.diff() {
                let resource = resource.as_ref();
                let public = async {
                    let data = options
                        .read(resource, Storage::Public)
                        .await?
                        .unwrap_or_default();
                    let mut data = data.as_ref().clone();

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
                    data = public_rewinder
                        .rewind(resource, data)
                        .map_or_else(|e| e.into_data(), identity);
                    options
                        .write(
                            resource,
                            WriteStorage::Public(
                                WriteMtime::No,
                                public_rewinder.last_change_to_resource(resource),
                            ),
                            Arc::new(data),
                            false,
                        )
                        .await?;
                    Ok(())
                };
                let current = async {
                    let data = options
                        .read(resource, Storage::Current)
                        .await?
                        .unwrap_or_default();
                    let mut data = data.as_ref().clone();

                    let result = if diff.apply_overlaps_adaptive_end(data.len()) {
                        let mut other = Vec::with_capacity(data.len() + 64);
                        let r = diff.apply_adaptive_end(&data, &mut other);
                        data = other;
                        r
                    } else {
                        diff.apply_in_place_adaptive_end(&mut data)
                    };
                    data = current_rewinder
                        .rewind(resource, data)
                        .map_or_else(|e| e.into_data(), identity);
                    if result.is_err() {
                        return Err(ApplicationError::PiersRejected);
                    }
                    options
                        .write(resource, WriteStorage::current(), Arc::new(data), true)
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
                warn!(
                    "Fast forward failed due to an error with applying \
                    the sync response. Trying again."
                );

                // UNWRAP: see the docs of `apply_sync_reply`.
                if let Some(ff) = manager.process_fast_forward().expect(
                    "BUG: Internal agde state error when \
                    trying fast forward again after pier failed us",
                ) {
                    drop(manager);
                    send(write, &ff).await?;
                }
                return Ok(());
            }

            for resource in sync.delete() {
                let resource = resource.as_ref();
                options.delete(resource, Storage::Public).await?;
            }

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
            info!("Fast forward complete.");
            {
                let mut metadata = options.metadata().lock().await;
                let mut offline_metadata = options.metadata_offline().lock().await;
                metadata_applier.apply(&mut metadata);
                metadata_applier.apply(&mut offline_metadata);
            }
            options.sync_metadata(Storage::Public).await?;
            options.sync_metadata(Storage::Current).await?;
        }
        agde::SyncReplyAction::HashCheck { unwinder } => {
            let mut public_unwinder = unwinder;
            let mut current_unwinder = public_unwinder.clone();
            // `TODO`: do this in parallel, but that uses more memory! Requires a streaming API.
            for (resource, diff) in sync.diff() {
                let resource = resource.as_ref();
                let public = async {
                    let data = options
                        .read(resource, Storage::Public)
                        .await?
                        .unwrap_or_default();
                    let data = data.as_ref().clone();

                    let mut data = public_unwinder
                        .unwind(data, resource)
                        .unwrap_or_else(|e| e.into_data());

                    if diff.apply_overlaps(data.len()) {
                        let mut other = Vec::with_capacity(data.len() + 64);
                        if diff.apply(&data, &mut other).is_ok() {
                            data = other;
                        }
                    } else if diff.in_bounds(&data) {
                        diff.apply_in_place(&mut data).unwrap();
                    };
                    data = public_unwinder
                        .rewind(data)
                        .map_or_else(|(_err, vec)| vec, identity);
                    options
                        .write(
                            resource,
                            WriteStorage::Public(
                                WriteMtime::No,
                                public_unwinder.last_change_to_resource(resource),
                            ),
                            Arc::new(data),
                            false,
                        )
                        .await?;
                    Ok(())
                };
                let current = async {
                    let data = options
                        .read(resource, Storage::Current)
                        .await?
                        .unwrap_or_default();
                    let data = data.as_ref().clone();

                    let mut data = current_unwinder
                        .unwind(data, resource)
                        .unwrap_or_else(agde::event::UnwindError::into_data);

                    if diff.apply_overlaps_adaptive_end(data.len()) {
                        let mut other = Vec::with_capacity(data.len() + 64);
                        if diff.apply_adaptive_end(&data, &mut other).is_ok() {
                            data = other;
                        }
                    } else if diff.in_bounds(&data) {
                        diff.apply_in_place_adaptive_end(&mut data).unwrap();
                    };
                    data = current_unwinder
                        .rewind(data)
                        .map_or_else(|(_err, vec)| vec, identity);
                    options
                        .write(resource, WriteStorage::current(), Arc::new(data), true)
                        .await?;
                    Ok(())
                };
                futures::future::try_join(public, current).await?;
            }

            for resource in sync.delete() {
                let resource = resource.as_ref();
                options.delete(resource, Storage::Public).await?;
            }
        }
        agde::SyncReplyAction::UnexpectedPier => {
            info!("Agde told us to ignore the sync reply.");
        }
    }

    Ok(())
}

fn from_compressed_bin(bytes: &[u8]) -> Result<agde::Message, ()> {
    let read = std::io::Cursor::new(bytes);
    let mut decompressor = snap::read::FrameDecoder::new(read);
    bincode::serde::decode_from_std_read(
        &mut decompressor,
        bincode::config::standard().write_fixed_array_length(),
    )
    .map_err(|_| ())
}
fn to_compressed_bin(message: &agde::Message) -> Vec<u8> {
    let mut buffer = Vec::new();
    let mut compressor = snap::write::FrameEncoder::new(&mut buffer);

    bincode::serde::encode_into_std_write(
        message,
        &mut compressor,
        bincode::config::standard().write_fixed_array_length(),
    )
    .unwrap();

    compressor.into_inner().expect("failed to write to a vec");
    buffer
}
async fn send(stream: &Mutex<WriteHalf>, message: &agde::Message) -> Result<(), ApplicationError> {
    let buffer = to_compressed_bin(message);
    stream
        .lock()
        .await
        .send(buffer.into())
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
