//! Code for the native (tokio+fs) implementation of agde.
//!
//! Already keeping this separate from the `agde-tokio` lib to make it easier to adapt this to the
//! web.

use std::io::{BufRead, BufReader, Read, Write};
use std::task::Poll;

use tokio::io::{AsyncRead, AsyncWrite};

use crate::*;

// OK to have lazy_static here since our native implementation only has one agde instance running.
lazy_static::lazy_static! {
    static ref STATE: std::sync::Mutex<Option<StateHandle<Native>>> = std::sync::Mutex::new(None);
}

pub struct TokioRuntime;
pub type WsStream =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;
#[derive(Debug)]
pub struct WriteHalf(futures::stream::SplitSink<WsStream, tungstenite::Message>);
#[derive(Debug)]
pub struct ReadHalf(futures::stream::SplitStream<WsStream>);
impl Sink<Message> for WriteHalf {
    type Error = <WsStream as Sink<tungstenite::Message>>::Error;

    fn poll_ready(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.0.poll_ready_unpin(cx)
    }
    fn start_send(mut self: Pin<&mut Self>, item: Message) -> Result<(), Self::Error> {
        let item = match item {
            Message::Text(msg) => tungstenite::Message::Text(msg),
            Message::Binary(msg) => tungstenite::Message::Binary(msg),
        };
        self.0.start_send_unpin(item)
    }
    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.0.poll_flush_unpin(cx)
    }
    fn poll_close(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.0.poll_close_unpin(cx)
    }
}
impl Stream for ReadHalf {
    type Item = Result<Message, ()>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let item = self.0.poll_next_unpin(cx);
        match item {
            Poll::Ready(Some(Ok(msg))) => Poll::Ready(Some(Ok(match msg {
                tungstenite::Message::Text(t) => Message::Text(t),
                tungstenite::Message::Binary(b) => Message::Binary(b),
                // ignore others
                _ => return Poll::Pending,
            }))),
            Poll::Ready(Some(Err(_))) => Poll::Ready(Some(Err(()))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Native(Arc<Mutex<WriteHalf>>, Arc<Mutex<ReadHalf>>);
pub struct TokioTaskHandle<T>(tokio::task::JoinHandle<T>);
impl<T> Future for TokioTaskHandle<T> {
    type Output = Result<T, JoinError>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.0.poll_unpin(cx).map_err(|_| JoinError)
    }
}
impl<T: Send> TaskHandle<T> for TokioTaskHandle<T> {
    fn abort(&mut self) {
        tokio::task::JoinHandle::abort(&self.0);
    }
}
impl Runtime for TokioRuntime {
    type Sleep = tokio::time::Sleep;

    fn spawn<T: Send + 'static, F: Future<Output = T> + Send + 'static>(
        future: F,
    ) -> Box<dyn TaskHandle<T>> {
        Box::new(TokioTaskHandle(tokio::spawn(future)))
    }

    fn sleep(duration: Duration) -> Self::Sleep {
        tokio::time::sleep(duration)
    }
}
impl Sender for WriteHalf {}
impl Receiver for ReadHalf {}
impl Platform for Native {
    type Sender = WriteHalf;
    type Receiver = ReadHalf;
    type Rt = TokioRuntime;

    fn sender(&self) -> &Mutex<Self::Sender> {
        &self.0
    }

    fn receiver(&self) -> &Mutex<Self::Receiver> {
        &self.1
    }
}

#[derive(Debug, Clone, Copy)]
pub enum Compression {
    None = 0,
    Zstd = 1,
    Snappy = 2,
}
impl Compression {
    fn from_bytes(bytes: &[u8]) -> Option<Self> {
        Some(match bytes.first()? {
            0 => Self::None,
            1 => Self::Zstd,
            2 => Self::Snappy,
            _ => return None,
        })
    }
}

struct AsyncReadBlocking<R: Read + Unpin>(R);
impl<R: Read + Unpin> AsyncRead for AsyncReadBlocking<R> {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        let mut filled = buf.filled().len();
        unsafe { buf.assume_init(buf.capacity()) };
        let unfilled = &mut buf.filled_mut()[filled..];
        let r = self.get_mut().0.read(unfilled);
        if let Ok(read) = &r {
            filled += read;
        }

        unsafe { buf.assume_init(filled) };
        Poll::Ready(r.map(|_| ()))
    }
}
struct AsyncWriteBlocking<R: Write + Unpin>(R);
impl<R: Write + Unpin> AsyncWrite for AsyncWriteBlocking<R> {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        Poll::Ready(self.get_mut().0.write(buf))
    }
    fn poll_flush(
        self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        Poll::Ready(self.get_mut().0.flush())
    }
    fn poll_shutdown(
        self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        Poll::Ready(self.get_mut().0.flush())
    }
}
struct AsyncReadBuf<R: AsyncRead + Unpin> {
    reader: R,
    buf: Vec<u8>,
    pos: usize,
}
impl<R: AsyncRead + Unpin> AsyncRead for AsyncReadBuf<R> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let me = self.get_mut();
        let buffer = me.buf.get(me.pos..).unwrap_or(&[]);
        if !buffer.is_empty() {
            let mut filled = buf.filled().len();
            unsafe { buf.assume_init(buf.capacity()) };
            let unfilled = &mut buf.filled_mut()[filled..];

            let len = unfilled.len().min(buffer.len());
            me.pos += len;
            unfilled[..len].copy_from_slice(&buffer[..len]);
            filled += len;

            unsafe { buf.assume_init(filled) };
            return Poll::Ready(Ok(()));
        }

        Pin::new(&mut me.reader).poll_read(cx, buf)
    }
}

async fn metadata_new(storage: Storage) -> Result<Metadata, io::Error> {
    let map: Result<HashMap<String, ResourceMeta>, io::Error> =
        tokio::task::spawn_blocking(move || {
            let mut map = HashMap::new();

            let path = match storage {
                Storage::Public => ".agde/files/",
                Storage::Meta => ".agde/",
                Storage::Current => "./",
            };

            for entry in walkdir::WalkDir::new(path)
                .follow_links(false)
                .into_iter()
                .filter_entry(|e| {
                    !(e.path().starts_with(".agde") || e.path().starts_with("./.agde"))
                })
                .filter_map(|e| e.ok())
            {
                if let Some(path) = entry.path().to_str() {
                    let metadata = entry.metadata()?;
                    if !metadata.is_file() {
                        continue;
                    }
                    // > 1GiB
                    if metadata.len() > 1024 * 1024 * 1024 {
                        warn!(
                            "Not tracking file {:?} because it's larger than 1 GiB",
                            entry.path()
                        );
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

async fn flush<P: Platform>(
    manager: &mut Manager,
    options: &Options<P>,
    platform: &PlatformExt<P>,
) -> Result<(), ApplicationError> {
    // ignore error on send if the connection is closed.
    let _ = platform.send(&manager.process_disconnect()).await;

    let state = options.read_clean().await?;
    if state.map_or(false, |state| &**state != b"y") && !options.public_storage_disabled() {
        error!("State not clean. Trying to apply diffs to current buffer.");

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
                        MetadataChange::Modify(resource, created, _) => {
                            let current = options.read(&resource, Storage::Current).await?.expect(
                                "configuration should not return Modified \
                                            if the Current storage version doesn't exist.",
                            );
                            let mut current = current.as_ref().clone();

                            let public = options
                                .read(&resource, Storage::Public)
                                .await?
                                .unwrap_or_default();
                            let public = public.as_ref().clone();

                            // cursors: we're catching ctrlc, so this isn't our highest
                            // priority
                            current = if let Some(current) = rewind_current(
                                &mut *manager,
                                created,
                                &resource,
                                public,
                                current,
                                &mut [],
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
                                .write(resource, WriteStorage::current(), Arc::new(current), true)
                                .await?;
                        }
                    }
                }
            }
        }
    }

    options.flush_out().await?;
    options.sync_metadata(Storage::Public).await?;
    options.sync_metadata(Storage::Current).await?;
    info!("Successfully flushed caches.");

    // we are clean again!
    options.write_clean("y", true).await?;

    Ok::<(), ApplicationError>(())
}

#[allow(clippy::await_holding_lock)]
pub async fn catch_ctrlc(handle: StateHandle<Native>) {
    let manager = Arc::clone(&handle.manager);
    let options = Arc::clone(&handle.options);
    let platform = handle.platform.clone();

    let handler = ctrlc::set_handler(move || {
        info!("Caught ctrlc");
        let manager = Arc::clone(&manager);
        let options = Arc::clone(&options);
        let platform = platform.clone();

        let runtime = tokio::runtime::Builder::new_current_thread()
            .max_blocking_threads(1)
            .build()
            .expect("failed to start tokio when handling ctrlc");
        let returned = runtime.block_on(async move {
            let mut manager = manager.lock().await;
            flush(&mut manager, &options, &platform).await
        });

        if let Err(err) = returned {
            error!("Error on ctrlc cleanup: {err:?}");
        }

        info!("Successfully cleaned up.");

        std::process::exit(0);
    });
    match handler {
        Err(ctrlc::Error::MultipleHandlers) => {
            let mut lock = STATE.lock().unwrap();
            if let Some(lock) = lock.as_mut() {
                {
                    let mut manager = lock.manager.lock().await;
                    if let Err(err) = flush(&mut manager, &lock.options, &lock.platform).await {
                        error!("Error when trying to flush previous manager: {err:?}");
                    }
                }
                *lock = handle;
            } else {
                warn!("Failed to set ctrlc handler: another handler has been set");
            }
        }
        Err(_) => {
            warn!("Failed to set ctrlc handler.");
        }
        Ok(_) => {
            let mut lock = STATE.lock().unwrap();
            *lock = Some(handle);
        }
    }
}

pub async fn options_fs(
    force_pull: bool,
    compression: Compression,
) -> Result<Options<Native>, io::Error> {
    let read = |resource, storage| {
        Box::pin(async move {
            let path = match storage {
                Storage::Public => format!(".agde/files/{resource}",),
                Storage::Current => format!("./{resource}"),
                Storage::Meta => format!(".agde/{resource}"),
            };
            tokio::task::spawn_blocking(move || {
                let mut file = match std::fs::File::open(&path) {
                    Ok(f) => BufReader::new(f),
                    Err(err) => match err.kind() {
                        io::ErrorKind::NotFound => return Ok(None),
                        _ => return Err(()),
                    },
                };

                let buf = if !matches!(storage, Storage::Current) {
                    let buffer = file.fill_buf().map_err(|_| ())?;
                    let compress = Compression::from_bytes(buffer);
                    // don't ignore the first if we have old storage which stored without the
                    // compression header.
                    if compress.is_some() {
                        file.read_exact(&mut [0; 1])
                            .expect("failed to read from buffer");
                    }
                    let compress = compress.unwrap_or(Compression::None);
                    match compress {
                        Compression::None => {
                            let mut buf = Vec::with_capacity(1024);
                            file.read_to_end(&mut buf).map_err(|_| ())?;
                            buf
                        }
                        Compression::Zstd => {
                            let mut decompressor = zstd::stream::read::Decoder::with_buffer(file)
                                .expect("Failed to create zstd decoder");
                            let mut decompressed_buf = Vec::with_capacity(1024);
                            let r = decompressor.read_to_end(&mut decompressed_buf);
                            if let Err(err) = r {
                                warn!(
                                    "Reading from agde public file failed. \
                                    Make sure you have permissions and didn't \
                                    modify the compressed files: {err:?}"
                                );
                                return Err(());
                            }

                            decompressed_buf
                        }
                        Compression::Snappy => {
                            let mut decompressor = snap::read::FrameDecoder::new(file);
                            let mut decompressed_buf = Vec::with_capacity(1024);
                            let r = decompressor.read_to_end(&mut decompressed_buf);
                            if r.is_err() {
                                warn!(
                                    "Reading from agde public file failed. \
                                        Make sure you have permissions and didn't \
                                        modify the compressed files."
                                );
                                return Err(());
                            }

                            decompressed_buf
                        }
                    }
                } else {
                    println!("Dont");
                    let mut buf = Vec::with_capacity(1024);
                    file.read_to_end(&mut buf).map_err(|_| ())?;
                    buf
                };

                Ok(Some(buf))
            })
            .await
            .unwrap()
        }) as ReadFuture
    };

    let metadata = initial_metadata(
        "metadata",
        || metadata_new(Storage::Current),
        force_pull,
        |res| read(res, Storage::Meta),
    )
    .await?;
    // A metadata cache that is held constant between diff calls.
    let offline_metadata = initial_metadata(
        "metadata-offline",
        || {
            // start of empty, as we aren't tracking any files yet.
            let meta = Metadata::new(HashMap::new());
            futures::future::ready(Ok(meta))
        },
        force_pull,
        |res| read(res, Storage::Meta),
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

    let write = move |resource: String, storage, data: Arc<Vec<u8>>| {
        let metadata = Arc::clone(&write_metadata);
        let offline_metadata = Arc::clone(&write_offline_metadata);
        Box::pin(async move {
            let path = match storage {
                WriteStorage::Public(_, _) => format!(".agde/files/{resource}",),
                WriteStorage::Current(_) => format!("./{resource}"),
                WriteStorage::Meta => format!(".agde/{resource}"),
            };

            if let Some(path) = Path::new(&path).parent() {
                tokio::fs::create_dir_all(path).await.map_err(|_| ())?;
            }
            if data.len() < 300 {
                info!("Writing to {path}, {:?}", String::from_utf8_lossy(&data));
            } else {
                info!("Writing to {path} with length {}", data.len());
            }

            let data_len = data.len();
            let file_path = path.clone();
            tokio::task::spawn_blocking(move || {
                let mut file = std::fs::File::create(&file_path).map_err(|_| ())?;
                if !matches!(storage, WriteStorage::Current(_)) {
                    file.write(&[compression as u8]).map_err(|_| ())?;
                    match compression {
                        Compression::None => {
                            file.write_all(&data).map_err(|_| ())?;
                        }
                        Compression::Zstd => {
                            let mut compressor = zstd::stream::write::Encoder::new(
                                &mut file,
                                zstd::DEFAULT_COMPRESSION_LEVEL,
                            )
                            .expect("Failed to create a zstd encoder.");
                            compressor.write_all(&data).map_err(|_| ())?;
                            compressor.finish().map_err(|_| ())?;
                        }
                        Compression::Snappy => {
                            let mut compressor = snap::write::FrameEncoder::new(&mut file);
                            compressor.write_all(&data).map_err(|_| ())?;
                        }
                    }
                } else {
                    file.write_all(&data).map_err(|_| ())?;
                }
                file.flush().map_err(|_| ())?;
                Ok(())
            })
            .await
            .unwrap()?;

            match storage {
                // meta storage obviously doesn't affect the files' metadata.
                WriteStorage::Meta => {}
                WriteStorage::Public(write_mtime, mut event_mtime) => {
                    let mut metadata = metadata.lock().await;

                    let mut mtime = match write_mtime {
                        WriteMtime::No => None,
                        WriteMtime::LookUpCurrent => {
                            if let Ok(metadata) = tokio::fs::metadata(format!("./{resource}")).await
                            {
                                let mtime = metadata.modified().map_err(|_| ())?;
                                Some(mtime)
                            } else {
                                None
                            }
                        }
                    };
                    if let Some(meta) = metadata.get(&resource) {
                        if meta.mtime_of_last_event() != SystemTime::UNIX_EPOCH {
                            event_mtime = meta.mtime_of_last_event();
                        }
                    }
                    if mtime.is_none() {
                        let offline_meta = { offline_metadata.lock().await.get(&resource) };
                        if let Some(meta) = offline_meta {
                            mtime = meta.mtime_in_current();
                        }
                    }
                    let meta = ResourceMeta::new_from_event(mtime, event_mtime, data_len as u64);
                    metadata.insert(resource, meta);
                }
                WriteStorage::Current(write_mtime) => match write_mtime {
                    WriteMtime::No => {}
                    WriteMtime::LookUpCurrent => {
                        let file_metadata = tokio::fs::metadata(&path).await.map_err(|_| ())?;
                        let mtime = file_metadata.modified().map_err(|_| ())?;
                        {
                            let mut metadata = offline_metadata.lock().await;

                            metadata.insert(
                                resource.clone(),
                                ResourceMeta::new(Some(mtime), data_len as u64),
                            );
                        }
                        {
                            let mut metadata = metadata.lock().await;

                            let mut event_mtime = SystemTime::UNIX_EPOCH;
                            if let Some(meta) = metadata.get(&resource) {
                                event_mtime = meta.mtime_of_last_event();
                            }
                            let meta = ResourceMeta::new_from_event(
                                Some(mtime),
                                event_mtime,
                                data_len as u64,
                            );
                            metadata.insert(resource, meta);
                        }
                    }
                },
            }
            Ok(())
        }) as WriteFuture
    };

    Ok(Options::new(
        Box::new(read),
        Box::new(write),
        Box::new(move |resource, storage| {
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
                    debug!("Removing {resource} from metadata cache.");
                    metadata.remove(&resource);
                }
                {
                    let mut metadata = metadata.lock().await;
                    metadata.remove(&resource);
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
        Box::new(move || {
            let metadata = Arc::clone(&diff_metadata);
            let offline_metadata = Arc::clone(&diff_offline_metadata);
            Box::pin(async move {
                debug!("Getting diff");
                let current_metadata = metadata_new(Storage::Current).await.map_err(|_| ())?;
                let mut offline_metadata = offline_metadata.lock().await;
                let changed = offline_metadata.changes(&current_metadata, true);
                let mut metadata = metadata.lock().await;
                metadata.apply_changes(&changed, &current_metadata);
                // `TODO`: Optimize this
                *offline_metadata = metadata.clone();
                debug!("Changed: {:?}", changed);
                Ok(changed)
            }) as DiffFuture
        }),
        metadata,
        offline_metadata,
        Duration::from_secs(7),
        Duration::from_secs(10),
        force_pull,
        true,
    ))
}

/// Watch for changes.
///
/// You have to call [`notify::Watcher::watch`] to start watching and
/// [`notify::Watcher::unwatch`] to stop the execution.
pub async fn watch_changes<CommitFut: Future<Output = ()> + Send>(
    commit: impl Fn() -> CommitFut + Send + Sync + 'static,
) -> impl notify::Watcher {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let watcher =
        notify::recommended_watcher(move |res| tx.send(res).expect("receiving end failed"))
            .expect("failed to start watcher");

    let in_timeout = Arc::new(std::sync::Mutex::new(false));

    let commit = Arc::new(commit);

    tokio::spawn(async move {
        while let Some(ev) = rx.recv().await {
            let ev = match ev {
                Ok(ev) => ev,
                Err(err) => {
                    warn!("Watcher failed: {err:?}");
                    continue;
                }
            };
            if ev.paths.first().map_or(false, |path| {
                path.components()
                    .any(|component| component.as_os_str() == ".agde")
            }) {
                continue;
            }
            // debounce
            if !{ *in_timeout.lock().unwrap() } {
                *in_timeout.lock().unwrap() = true;
                let commit = Arc::clone(&commit);
                let in_timeout = Arc::clone(&in_timeout);
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_millis(100)).await;

                    commit().await;
                    *in_timeout.lock().unwrap() = false;
                });
            }
        }
        // just end the task
    });
    watcher
}

pub async fn connect_ws(url: &str) -> Result<Native, ApplicationError> {
    info!("Connecting to {url:?}.");
    let result = tokio_tungstenite::connect_async(url).await;
    let conenction = result.map_err(|err| ApplicationError::ConnectionFailed(err.to_string()))?;
    let (w, r) = conenction.0.split();
    Ok(Native(
        Arc::new(Mutex::new(WriteHalf(w))),
        Arc::new(Mutex::new(ReadHalf(r))),
    ))
}
async fn initial_metadata<
    F: Future<Output = Result<Metadata, io::Error>>,
    ReadF: Future<Output = Result<Option<Vec<u8>>, ()>>,
>(
    name: &str,
    new: impl Fn() -> F,
    force_pull: bool,
    read: impl Fn(String) -> ReadF,
) -> Result<Metadata, io::Error> {
    tokio::fs::create_dir_all(".agde").await?;
    let metadata = read(name.to_owned())
        .then(|data| async move {
            match data {
                Ok(v) => match v {
                    Some(v) => Ok(v),
                    None => Err(false),
                },
                Err(()) => Err(true),
            }
        })
        .and_then(|data| async move {
            bincode::serde::decode_from_slice::<Metadata, _>(
                &data,
                bincode::config::standard().write_fixed_array_length(),
            )
            .map_err(|_| true)
        });
    match metadata.await {
        Ok((metadata, _)) => {
            let mut metadata: Metadata = metadata;
            let mut differing = Vec::new();
            for (resource, _metadata) in metadata.iter() {
                let meta = tokio::fs::metadata(format!(".agde/files/{resource}")).await;
                match meta {
                    Ok(_meta) => {
                        // don't do metadata checks, as compression changes the size
                        //
                        // if meta.len() != metadata.size() {
                        // differing.push(resource.to_owned());
                        // error!("File {resource} has different length in cached metadata and on disk.");
                        // }
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
        Err(hard_error) => {
            if hard_error {
                error!("Metadata corrupt. Recreating.");
            }

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

            if !populated || !hard_error || force_pull {
                new().await
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
