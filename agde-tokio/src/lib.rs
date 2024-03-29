//! Code for the native (tokio+fs) implementation of agde.
//!
//! Already keeping this separate from the `agde-tokio` lib to make it easier to adapt this to the
//! web.

use std::collections::HashMap;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime};

use agde::fast_forward::{Metadata, MetadataChange, ResourceMeta};
use agde::Manager;
use futures::lock::Mutex;
use futures::{Future, FutureExt, Sink, SinkExt, Stream, StreamExt, TryFutureExt};
use log::{debug, error, info, warn};
use tokio::io::{AsyncRead, AsyncWrite};
pub use tokio_tungstenite;
pub use tokio_tungstenite::tungstenite;

use agde_io::*;

pub use agde;
pub use agde_io;

// OK to have lazy_static here since our native implementation only has one agde instance running.
lazy_static::lazy_static! {
    static ref STATE: std::sync::Mutex<Option<StateHandle<Native>>> = std::sync::Mutex::new(None);
}

#[derive(Debug)]
#[allow(clippy::large_enum_variant)] // these are anyway going to be in Arcs, so the size isn't
                                     // that important!
pub enum Io {
    Ws(
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
    ),
    Duplex(tokio_tungstenite::WebSocketStream<tokio::io::DuplexStream>),
}
impl Sink<tungstenite::Message> for Io {
    type Error = tungstenite::Error;
    fn start_send(mut self: Pin<&mut Self>, item: tungstenite::Message) -> Result<(), Self::Error> {
        match &mut *self {
            Self::Ws(ws) => ws.start_send_unpin(item),
            Self::Duplex(dx) => dx.start_send_unpin(item),
        }
    }
    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        match &mut *self {
            Self::Ws(ws) => ws.poll_ready_unpin(cx),
            Self::Duplex(dx) => dx.poll_ready_unpin(cx),
        }
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        match &mut *self {
            Self::Ws(ws) => ws.poll_flush_unpin(cx),
            Self::Duplex(dx) => dx.poll_flush_unpin(cx),
        }
    }
    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        match &mut *self {
            Self::Ws(ws) => ws.poll_close_unpin(cx),
            Self::Duplex(dx) => dx.poll_close_unpin(cx),
        }
    }
}
impl Stream for Io {
    type Item = tungstenite::Result<tungstenite::Message>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match &mut *self {
            Self::Ws(ws) => ws.poll_next_unpin(cx),
            Self::Duplex(dx) => dx.poll_next_unpin(cx),
        }
    }
}

pub struct TokioRuntime;
#[derive(Debug)]
pub struct WriteHalf(pub futures::stream::SplitSink<Io, tungstenite::Message>);
#[derive(Debug)]
pub struct ReadHalf(pub futures::stream::SplitStream<Io>);
impl Sink<Message> for WriteHalf {
    type Error = <Io as Sink<tungstenite::Message>>::Error;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.0.poll_ready_unpin(cx)
    }
    fn start_send(mut self: Pin<&mut Self>, item: Message) -> Result<(), Self::Error> {
        let item = match item {
            Message::Text(msg) => tungstenite::Message::Text(msg),
            Message::Binary(msg) => tungstenite::Message::Binary(msg),
        };
        self.0.start_send_unpin(item)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.0.poll_flush_unpin(cx)
    }
    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.0.poll_close_unpin(cx)
    }
}
impl Stream for ReadHalf {
    type Item = Result<Message, ()>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
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
pub struct Native(pub Arc<Mutex<WriteHalf>>, pub Arc<Mutex<ReadHalf>>);
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
        _cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
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
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        Poll::Ready(self.get_mut().0.write(buf))
    }
    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        Poll::Ready(self.get_mut().0.flush())
    }
    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
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
        cx: &mut Context<'_>,
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

fn path_from_storage(storage: Storage, resource: &str, base_path: &Path) -> PathBuf {
    let mut path = PathBuf::with_capacity(20 + resource.len());
    path.push(base_path);
    match storage {
        Storage::Public => path.push(".agde/files"),
        Storage::Current => {}
        Storage::Meta => path.push(".agde"),
    }
    path.push(resource);
    path
}

async fn metadata_new(storage: Storage, base_path: PathBuf) -> Result<Metadata, io::Error> {
    let map: Result<HashMap<String, ResourceMeta>, io::Error> =
        tokio::task::spawn_blocking(move || {
            let mut map = HashMap::new();

            let path = path_from_storage(storage, "./", &base_path);

            for entry in walkdir::WalkDir::new(path)
                .follow_links(false)
                .into_iter()
                .filter_entry(|e| {
                    !(e.path().starts_with(".agde") || e.path().starts_with("./.agde"))
                })
                .filter_map(|e| e.ok())
            {
                if let Some(mut path) = entry.path().to_str() {
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

                    if let Some(p) = path.strip_prefix("./") {
                        path = p
                    }

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

pub async fn shutdown<P: Platform>(
    manager: &mut Manager,
    options: &Options<P>,
    platform: &PlatformExt<P>,
    handle: &StateHandle<P>,
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
                            current = if let Some(current) = agde_io::rewind_current(
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
    handle.abort_tasks();

    // we are clean again!
    options.write_clean("y", true).await?;

    Ok::<(), ApplicationError>(())
}

#[allow(clippy::await_holding_lock)]
pub async fn catch_ctrlc(handle: StateHandle<Native>) {
    let manager = Arc::clone(&handle.manager);
    let options = Arc::clone(&handle.options);
    let platform = handle.platform.clone();

    let handler_handle = handle.clone();
    let handler = ctrlc::set_handler(move || {
        info!("Caught ctrlc");
        let manager = Arc::clone(&manager);
        let options = Arc::clone(&options);
        let platform = platform.clone();

        let runtime = tokio::runtime::Builder::new_current_thread()
            .max_blocking_threads(1)
            .build()
            .expect("failed to start tokio when handling ctrlc");
        let handle = handler_handle.clone();
        let returned = runtime.block_on(async move {
            let mut manager = manager.lock().await;
            shutdown(&mut manager, &options, &platform, &handle).await
        });

        if let Err(err) = returned {
            error!("Error on ctrlc cleanup: {err:?}");
        }

        info!("Successfully cleaned up.");

        std::process::exit(0);
    });
    match handler {
        Err(ctrlc::Error::MultipleHandlers) => {
            let handle = {
                let mut state_lock = STATE.lock().unwrap();
                if let Some(lock) = state_lock.as_mut() {
                    *lock = handle;
                    lock.clone()
                } else {
                    warn!("Failed to set ctrlc handler: another handler has been set");
                    return;
                }
            };
            {
                let mgr = handle.manager.clone();
                let mut manager = mgr.lock().await;
                if let Err(err) =
                    shutdown(&mut manager, &handle.options, &handle.platform, &handle).await
                {
                    error!("Error when trying to flush previous manager: {err:?}");
                }
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
    base_path: PathBuf,
) -> Result<Options<Native>, io::Error> {
    let read_base = base_path.clone();
    let read = move |resource: String, storage| {
        let read_base = read_base.clone();
        Box::pin(async move {
            let path = path_from_storage(storage, &resource, &read_base);
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
        &base_path,
        || metadata_new(Storage::Current, base_path.clone()),
        force_pull,
        |res| read(res, Storage::Meta),
    )
    .await?;
    // A metadata cache that is held constant between diff calls.
    let offline_metadata = initial_metadata(
        "metadata-offline",
        &base_path,
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

    let write_base = base_path.clone();
    let write = move |resource: String, storage: WriteStorage, data: Arc<Vec<u8>>| {
        let metadata = Arc::clone(&write_metadata);
        let offline_metadata = Arc::clone(&write_offline_metadata);
        let write_base = write_base.clone();
        Box::pin(async move {
            let path = path_from_storage(storage.to_storage(), &resource, &write_base);

            if let Some(path) = Path::new(&path).parent() {
                tokio::fs::create_dir_all(path).await.map_err(|_| ())?;
            }
            if data.len() < 300 {
                info!(
                    "Writing to {}, {:?}",
                    path.display(),
                    String::from_utf8_lossy(&data)
                );
            } else {
                info!("Writing to {} with length {}", path.display(), data.len());
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

            storage
                .update_metadata(
                    &metadata,
                    &offline_metadata,
                    resource,
                    data_len,
                    |storage, resource| async move {
                        let path = path_from_storage(storage, &resource, &write_base);
                        tokio::fs::metadata(path).await.ok().and_then(|m| {
                            m.modified()
                                .map_err(|_| error!("Failed to get mtime of resource {resource}"))
                                .ok()
                        })
                    },
                )
                .await?;
            Ok(())
        }) as WriteFuture
    };

    let remove_base = base_path.clone();
    let diff_base = base_path.clone();
    Ok(Options::new(
        Box::new(read),
        Box::new(write),
        Box::new(move |resource, storage| {
            let metadata = Arc::clone(&delete_metadata);
            let offline_metadata = Arc::clone(&delete_offline_metadata);
            let remove_base = remove_base.clone();
            Box::pin(async move {
                let path = path_from_storage(storage, &resource, &remove_base);
                debug!("Removing {resource} in {storage} from metadata cache.");
                storage
                    .delete_update_metadata(&metadata, &offline_metadata, &resource)
                    .await;
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
            let diff_base = diff_base.clone();
            Box::pin(async move {
                debug!("Getting diff");
                let current_metadata = metadata_new(Storage::Current, diff_base.clone())
                    .await
                    .map_err(|_| ())?;
                let mut offline_metadata = offline_metadata.lock().await;
                let changed = offline_metadata.changes(&current_metadata, true);
                let mut metadata = metadata.lock().await;
                metadata.apply_changes(&changed, &current_metadata);
                offline_metadata.clone_from(&metadata);
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

/// Use `host` to override the host header for the request.
pub async fn connect_ws(url: &str, host: Option<&str>) -> Result<Native, ApplicationError> {
    use tungstenite::client::IntoClientRequest;
    let mut req = url.into_client_request().unwrap();
    if let Some(host) = host {
        req.headers_mut().insert(
            "host",
            tungstenite::http::HeaderValue::from_str(host).expect("custom host header is invalid"),
        );
    }
    info!("Connecting to {url:?} with request {req:#?}.");
    let result = tokio_tungstenite::connect_async(req).await;
    let (connection, _) =
        result.map_err(|err| ApplicationError::ConnectionFailed(err.to_string()))?;
    let (w, r) = Io::Ws(connection).split();
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
    base_path: &Path,
    new: impl Fn() -> F,
    force_pull: bool,
    read: impl Fn(String) -> ReadF,
) -> Result<Metadata, io::Error> {
    tokio::fs::create_dir_all(base_path.join(".agde")).await?;
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
                let mut path = base_path.join(".agde/files");
                path.push(resource);
                let meta = tokio::fs::metadata(path).await;
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
