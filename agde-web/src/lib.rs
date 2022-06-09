#![allow(clippy::unused_unit)] // wasm_bindgen
use std::collections::HashMap;
use std::fmt::{self, Debug};
use std::io::{self, Read, Write};
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime};

use agde::fast_forward::{Metadata, MetadataChange, ResourceMeta};
use futures::channel::oneshot;
use futures::future::Either;
use futures::lock::Mutex;
use futures::{Future, FutureExt, Sink, SinkExt, Stream, StreamExt, TryFutureExt};
use gloo_net::websocket;
use js_sys::{Array, Function, Object, Promise, Reflect};
use log::{debug, error, info, warn};
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::JsFuture;

use agde_io::*;

#[wasm_bindgen]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Handle {
    id: u64,
}
#[wasm_bindgen]
impl Handle {
    #[wasm_bindgen]
    pub async fn wait_for_shutdown(self) -> Self {
        let handle = {
            HANDLES
                .lock()
                .await
                .remove(&self)
                .expect("shutting down a handle that doesn't exist")
        };
        handle
            .wait()
            .await
            .expect("failed to wait for agde shutdown");
        self
    }
    #[wasm_bindgen]
    pub async fn commit_and_send(self, cursors: Box<[JsValue]>) -> Result<JsValue, JsValue> {
        let handle = {
            let lock = HANDLES.lock().await;
            lock.get(&self)
                .expect("shutting down a handle that doesn't exist")
                .state()
                .clone()
        };
        let resource_key = JsValue::from_str("resource");
        let index_key = JsValue::from_str("index");
        let cursors = cursors
            .into_vec()
            .into_iter()
            .map(|f| {
                let obj: Object = f.dyn_into().expect("cursors array doesn't contain objects");
                let resource = Reflect::get(&obj, &resource_key)
                    .expect("resource item missing from cursor object")
                    .as_string()
                    .expect("resource in cursors isn't a string");
                let index = Reflect::get(&obj, &index_key)
                    .expect("index item missing from cursor object")
                    .as_f64()
                    .expect("index in cursors isn't a string") as usize;
                (resource, index)
            })
            .collect::<Vec<_>>();
        let mut cursors = cursors
            .iter()
            .map(|(res, idx)| Cursor {
                resource: res,
                index: *idx,
            })
            .collect::<Vec<_>>();
        handle
            .commit_and_send(&mut cursors)
            .await
            .map_err(|err| JsValue::from_str(&err.to_string()))?;

        let cursors = cursors
            .iter()
            .map(|cursor| {
                let object = Object::new();
                Reflect::set(&object, &resource_key, &JsValue::from_str(cursor.resource)).unwrap();
                let index = cursor.index.to_string();
                Reflect::set(&object, &index_key, &JsValue::from_str(&index)).unwrap();
                object.dyn_into::<JsValue>().unwrap()
            })
            .collect::<Array>();

        let obj = Object::new();
        Reflect::set(&obj, &JsValue::from_str("handle"), &self.into()).unwrap();
        Reflect::set(&obj, &JsValue::from_str("cursors"), &cursors).unwrap();

        Ok(AsRef::<JsValue>::as_ref(&obj).clone())
    }
    #[wasm_bindgen]
    pub async fn flush(self) -> Result<Handle, JsValue> {
        info!("Got flush!");
        let handle = HANDLES.lock().await;
        let me = handle
            .get(&self)
            .expect("flushing a handle that doesn't exist");
        info!("Got handle");
        me.state()
            .options
            .flush_out()
            .await
            .map_err(|err| JsValue::from_str(&err.to_string()))?;
        Ok(self)
    }
    #[wasm_bindgen]
    pub async fn shutdown(self) -> Result<Handle, JsValue> {
        async {
            {
                let handle = {
                    HANDLES
                        .lock()
                        .await
                        .remove(&self)
                        .expect("shutting down a handle that doesn't exist")
                };
                let handle = handle.state();
                let platform = &handle.platform;
                let mut manager = handle.manager.lock().await;
                let options = &handle.options;
                // ignore error on send if the connection is closed.
                let _ = platform.send(&manager.process_disconnect()).await;

                let state = options.read_clean().await?;
                if state.map_or(false, |state| &**state != b"y")
                    && !options.public_storage_disabled()
                {
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
                                        let current = options
                                            .read(&resource, Storage::Current)
                                            .await?
                                            .expect(
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
                                            .write(
                                                resource,
                                                WriteStorage::current(),
                                                Arc::new(current),
                                                true,
                                            )
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

                Ok(())
            }
        }
        .await
        .map_err(|err: ApplicationError| JsValue::from_str(&err.to_string()))
        .map(|()| self)
    }
}

lazy_static::lazy_static!(
    static ref HANDLES: Mutex<HashMap<Handle, agde_io::Handle<Web>>> = Mutex::new(HashMap::new());
);

pub struct WebRuntime;
pub type WsStream = gloo_net::websocket::futures::WebSocket;
pub struct WriteHalf(futures::stream::SplitSink<WsStream, websocket::Message>);
impl Debug for WriteHalf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("WriteHalf { internal websocket }")
    }
}
// these will only be accessed on the JS thread, and are always behind mutexes.
unsafe impl Sync for WriteHalf {}
unsafe impl Send for WriteHalf {}
pub struct ReadHalf(futures::stream::SplitStream<WsStream>);
impl Debug for ReadHalf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ReadHalf { internal websocket }")
    }
}
// these will only be accessed on the JS thread, and are always behind mutexes.
unsafe impl Sync for ReadHalf {}
unsafe impl Send for ReadHalf {}
impl Sink<Message> for WriteHalf {
    type Error = <WsStream as Sink<websocket::Message>>::Error;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.0.poll_ready_unpin(cx)
    }
    fn start_send(mut self: Pin<&mut Self>, item: Message) -> Result<(), Self::Error> {
        let item = match item {
            Message::Text(msg) => websocket::Message::Text(msg),
            Message::Binary(msg) => websocket::Message::Bytes(msg),
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
                websocket::Message::Text(t) => Message::Text(t),
                websocket::Message::Bytes(b) => Message::Binary(b),
            }))),
            Poll::Ready(Some(Err(_))) => Poll::Ready(Some(Err(()))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}
pub struct Sleep(gloo_timers::future::TimeoutFuture);
// these will only be accessed on the JS thread
unsafe impl Sync for Sleep {}
unsafe impl Send for Sleep {}
impl Future for Sleep {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.0.poll_unpin(cx)
    }
}

#[derive(Debug, Clone)]
pub struct Web(Arc<Mutex<WriteHalf>>, Arc<Mutex<ReadHalf>>);
pub struct WebTaskHandle<T> {
    resolve: oneshot::Receiver<Option<T>>,
    abort: Option<oneshot::Sender<()>>,
}
impl<T> Future for WebTaskHandle<T> {
    type Output = Result<T, JoinError>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.resolve
            .poll_unpin(cx)
            .map_err(|_| JoinError)
            .map(|r| r.and_then(|o| o.ok_or(JoinError)))
    }
}
impl<T: Send> TaskHandle<T> for WebTaskHandle<T> {
    fn abort(&mut self) {
        if let Some(abort) = self.abort.take() {
            abort.send(()).unwrap()
        }
    }
}
impl Runtime for WebRuntime {
    type Sleep = Sleep;

    fn spawn<T: Send + Unpin + 'static, F: Future<Output = T> + Send + 'static>(
        future: F,
    ) -> Box<dyn TaskHandle<T>> {
        let (a_tx, a_rx) = oneshot::channel();
        let (r_tx, r_rx) = oneshot::channel();
        let future = async move {
            match futures::future::select(Box::pin(future), a_rx).await {
                Either::Left((t, _)) => {
                    let _ = r_tx.send(Some(t));
                }
                Either::Right((r, other)) => {
                    if let Err(futures::channel::oneshot::Canceled) = r {
                        let _ = r_tx.send(Some(other.await));
                    } else {
                        let _ = r_tx.send(None);
                    }
                }
            }
        };
        wasm_bindgen_futures::spawn_local(future);
        Box::new(WebTaskHandle {
            abort: Some(a_tx),
            resolve: r_rx,
        })
    }

    fn sleep(duration: Duration) -> Self::Sleep {
        Sleep(gloo_timers::future::sleep(duration))
    }
}
impl Sender for WriteHalf {}
impl Receiver for ReadHalf {}
impl Platform for Web {
    type Sender = WriteHalf;
    type Receiver = ReadHalf;
    type Rt = WebRuntime;

    fn sender(&self) -> &Mutex<Self::Sender> {
        &self.0
    }

    fn receiver(&self) -> &Mutex<Self::Receiver> {
        &self.1
    }
}

// durations in f64 seconds
//
// new_manager(help_desire: number): Manager (wasm_bindgen)

// resource is public/{resource} for Storage::Public, etc.
// DB: {
//      [resource: string]:
//      {
//          data: string (BASE64),
//          compression: string("none"|"snappy"),
//          mtime: number,
//          size: number,
//      }
//  }
//
// async read_callback: (resource): null|{ compression: string, data: string (BASE64) }
// this should update the mtime (the number of seconds since UNIX_EPOCH)
// write_callback: (resource, data: string (base64), compression: string, size: number)
// delete_callback: (resource)
// async get_mtime: (resource): null | number (date)
// async list_all: (): { [resource: string]: { size: number } }
//
// new_options(read_callback, write_callback, delete_callback, get_mtime, list_all): Options
// (wasm_bindgen)
//
// run(Options, Manager, url: string): null | Handle (error! the result)
// commit_and_send(Handle)
// flush(Handle)
//
// on run, return handle with commit_and_send() (async, js promise
// <https://rustwasm.github.io/docs/wasm-bindgen/reference/js-promises-and-rust-futures.html>)

#[wasm_bindgen]
pub fn init_agde() {
    fn now_handler() -> SystemTime {
        let s = js_sys::Date::now() / 1000.;
        SystemTime::UNIX_EPOCH + Duration::from_secs_f64(s)
    }
    wasm_logger::init(wasm_logger::Config::new(log::Level::Info));
    console_error_panic_hook::set_once();
    unsafe { agde::utils::set_now_handler(now_handler) };
}
#[wasm_bindgen]
#[allow(clippy::too_many_arguments)]
pub async fn run(
    force_pull: bool,
    compression: String,
    read_callback: Function,
    write_callback: Function,
    delete_callback: Function,
    get_mtime: Function,
    list_all: Function,
    url: String,
    help_desire: i16,
) -> Result<Handle, JsValue> {
    use rand::Rng;

    let compression = compression.parse().expect("invalid compression value");
    let options = options_js_callback(
        force_pull,
        compression,
        read_callback,
        write_callback,
        delete_callback,
        get_mtime,
        list_all,
    )
    .await
    .map_err(|err| JsValue::from_str(&err.to_string()))?;
    let options = options
        .with_startup_duration(Duration::from_secs(1))
        .with_periodic_interval(Duration::from_secs(10));
    let manager = agde::Manager::new(false, help_desire, Duration::from_secs(60), 512);
    let handle_id = manager.rng().gen();
    let handle = agde_io::run(manager, options.arc(), || connect_ws(&url))
        .await
        .map_err(|err| JsValue::from_str(&err.to_string()))?;
    let handle_id = Handle { id: handle_id };
    HANDLES.lock().await.insert(handle_id.clone(), handle);
    Ok(handle_id)
}

#[derive(Debug, Clone, Copy)]
pub enum Compression {
    None,
    Snappy,
}
impl Compression {
    fn to_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Snappy => "snappy",
        }
    }
}
impl FromStr for Compression {
    type Err = ();
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "none" => Self::None,
            "snappy" => Self::Snappy,
            _ => return Err(()),
        })
    }
}

async fn metadata_new<MtimeFuture: Future<Output = Option<SystemTime>>>(
    storage: Storage,
    resources: impl Future<Output = Vec<(String, u64)>>,
    mtime_of: &impl Fn(String, Storage) -> MtimeFuture,
) -> Result<Metadata, io::Error> {
    let resources = resources.await;
    let mut map = HashMap::with_capacity(resources.len() / 2);

    for (resource, size) in resources {
        let start_with = match storage {
            Storage::Public => "public/",
            Storage::Current => "current/",
            Storage::Meta => "meta/",
        };
        let resource = if let Some(sufix) = resource.strip_prefix(start_with) {
            sufix.to_owned()
        } else {
            continue;
        };
        if size > 1024 * 1024 * 1024 {
            warn!("Not tracking file {resource:?} because it's larger than 1 GiB",);
            continue;
        }
        let mtime = mtime_of(resource.clone(), storage).await;
        map.insert(resource, ResourceMeta::new(mtime, size));
    }

    Ok(Metadata::new(map))
}

fn path_from_storage(storage: Storage, resource: &str) -> String {
    match storage {
        Storage::Public => format!("public/{resource}"),
        Storage::Current => format!("current/{resource}"),
        Storage::Meta => format!("meta/{resource}"),
    }
}

struct JsFn(Function);
// these will only be accessed on the JS thread, and are always behind mutexes.
unsafe impl Sync for JsFn {}
unsafe impl Send for JsFn {}
impl JsFn {
    fn call(&self, args: &[JsValue]) -> Result<JsValue, JsValue> {
        let array = Array::from_iter(args.iter());
        self.0.apply(&JsValue::UNDEFINED, &array)
    }
}
struct SendPromise(JsFuture);
impl SendPromise {
    fn new(promise: JsFuture) -> Self {
        Self(promise)
    }
}
impl Future for SendPromise {
    type Output = Result<JsValueSend, JsValueSend>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.0.poll_unpin(cx).map(|r| match r {
            Ok(v) => Ok(JsValueSend(v)),
            Err(v) => Err(JsValueSend(v)),
        })
    }
}
// these will only be accessed on the JS thread, and are always behind mutexes.
unsafe impl Sync for SendPromise {}
unsafe impl Send for SendPromise {}
#[derive(Debug)]
struct JsValueSend(JsValue);
// these will only be accessed on the JS thread, and are always behind mutexes.
unsafe impl Sync for JsValueSend {}
unsafe impl Send for JsValueSend {}

pub async fn options_js_callback(
    force_pull: bool,
    compression: Compression,
    read_callback: Function,
    write_callback: Function,
    delete_callback: Function,
    get_mtime: Function,
    list_all: Function,
) -> Result<Options<Web>, io::Error> {
    let read_callback = Arc::new(Mutex::new(JsFn(read_callback)));
    let read_read_callback = read_callback.clone();
    let write_callback = Arc::new(Mutex::new(JsFn(write_callback)));
    let write_write_callback = write_callback.clone();
    let get_mtime = Arc::new(Mutex::new(JsFn(get_mtime)));
    let mtime_get_mtime = get_mtime.clone();
    let delete_callback = Arc::new(Mutex::new(JsFn(delete_callback)));
    let delete_delete_callback = delete_callback.clone();
    let list_all = Arc::new(Mutex::new(JsFn(list_all)));

    let mtime_of = move |resource: String, storage| {
        let path = path_from_storage(storage, &resource);
        let get_mtime = mtime_get_mtime.clone();
        async move {
            let promise = {
                let lock = get_mtime.lock().await;
                let path = JsValue::from_str(&path);
                let promise: Promise = lock
                    .call(&[path])
                    .expect("failed to get mtime")
                    .dyn_into()
                    .expect("get_mtime function didn't return promise");
                SendPromise::new(JsFuture::from(promise))
            };
            let result = promise.await;
            let result = result.expect("get_mtime js future threw");
            if result.0.is_null() {
                None
            } else {
                let seconds = result
                    .0
                    .as_f64()
                    .expect("get_mtime didn't return null or a number");
                Some(SystemTime::UNIX_EPOCH + Duration::from_secs_f64(seconds))
            }
        }
    };
    let mtime_of = Arc::new(mtime_of);
    let write_mtime_of = mtime_of.clone();
    let diff_mtime_of = mtime_of.clone();

    let list_all = move || {
        let list_all = list_all.clone();
        async move {
            let promise = {
                let lock = list_all.lock().await;
                let promise: Promise = lock
                    .call(&[])
                    .expect("failed to call list_all callback")
                    .dyn_into()
                    .expect("list_all function didn't return promise");
                SendPromise::new(JsFuture::from(promise))
            };
            let result = promise.await;
            let result = result.expect("list_all js future threw");
            let object: Object = result
                .0
                .dyn_into()
                .expect("list_all didn't return an object");
            let size_key = JsValue::from_str("size");
            Object::entries(&object)
                .iter()
                .map(|v| {
                    let array: Array = v
                        .dyn_into()
                        .expect("Object.entries returns an array of arrays");
                    let resource = array.get(0);
                    let resource = resource
                        .as_string()
                        .expect("key of a object should be strings");
                    let content = array.get(1);
                    let object: Object = content
                        .dyn_into()
                        .expect("resource DB doesn't have associated data");
                    let size = Reflect::get(&object, &size_key)
                        .expect("resource DB doesn't have the size for a resource");
                    let size = size.as_f64().expect("size of resource isn't a number") as u64;
                    (resource, size)
                })
                .collect::<Vec<_>>()
        }
    };
    let list_all = Arc::new(list_all);
    let diff_list_all = Arc::clone(&list_all);

    let read = move |resource: String, storage| {
        let read_callback = read_read_callback.clone();
        Box::pin(async move {
            let path = path_from_storage(storage, &resource);
            let read = read_callback.lock().await;
            let file_promise = {
                let file = read
                    .call(&[JsValue::from_str(&path)])
                    .expect("reading a resource failed when calling a read function");
                drop(read);
                let promise: Promise = file
                    .dyn_into()
                    .expect("read function didn't return promise");
                SendPromise::new(JsFuture::from(promise))
            };
            let file = file_promise.await.expect("the read future threw an error");

            if file.0.is_null() || file.0.is_undefined() {
                return Ok(None);
            }
            let data = Reflect::get(&file.0, &JsValue::from_str("data"))
                .expect("read promise didn't return an object with a file property")
                .as_string()
                .expect("file property isn't a string");
            let data = base64::decode(&data).expect("JS promise didn't return what we sent it");

            let buf = if !matches!(storage, Storage::Current) {
                let compression = Reflect::get(&file.0, &JsValue::from_str("compression"))
                    .expect("read promise didn't return an object with a file property")
                    .as_string()
                    .expect("file property isn't a string");
                let compression = compression
                    .parse()
                    .expect("read promise returned faulty compression property");
                match compression {
                    Compression::None => data,
                    Compression::Snappy => {
                        let cursor = io::Cursor::new(data);
                        let mut decompressor = snap::read::FrameDecoder::new(cursor);
                        let mut decompressed_buf = Vec::with_capacity(1024);
                        let r = decompressor.read_to_end(&mut decompressed_buf);
                        if r.is_err() {
                            warn!("Reading a file failed. JS might have tempered with it.");
                            return Err(());
                        }

                        decompressed_buf
                    }
                }
            } else {
                data
            };

            Ok(Some(buf))
        }) as ReadFuture
    };

    let metadata = initial_metadata(
        "metadata",
        || metadata_new(Storage::Current, list_all(), &*mtime_of),
        force_pull,
        |res| read(res, Storage::Meta),
        |res| mtime_of(res, Storage::Public),
        async { !list_all().await.is_empty() },
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
        |res| mtime_of(res, Storage::Public),
        async { !list_all().await.is_empty() },
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

    let write = move |resource: String, storage: WriteStorage, data: Arc<Vec<u8>>| {
        let metadata = Arc::clone(&write_metadata);
        let offline_metadata = Arc::clone(&write_offline_metadata);
        let write_callback = write_write_callback.clone();
        let mtime_of = write_mtime_of.clone();
        Box::pin(async move {
            let path = path_from_storage(storage.to_storage(), &resource);

            if data.len() < 300 {
                info!("Writing to {path}, {:?}", String::from_utf8_lossy(&data));
            } else {
                info!("Writing to {path} with length {}", data.len());
            }

            let data_len = data.len();
            let data = if !matches!(storage, WriteStorage::Current(_)) {
                match compression {
                    Compression::None => data,
                    Compression::Snappy => {
                        let mut buf = Vec::with_capacity(1024);
                        let mut compressor = snap::write::FrameEncoder::new(&mut buf);
                        compressor.write_all(&data).map_err(|_| ())?;
                        compressor.flush().map_err(|_| ())?;
                        drop(compressor);
                        Arc::new(buf)
                    }
                }
            } else {
                data
            };
            {
                let len = data.len();
                let write = write_callback.lock().await;
                let base64_data = base64::encode(data.as_slice());
                let data = JsValue::from_str(&base64_data);
                // free memory
                drop(base64_data);
                let compression = compression.to_str();
                let compression = JsValue::from_str(compression);
                let size = JsValue::from_f64(len as f64);

                write
                    .call(&[JsValue::from_str(&path), data, compression, size])
                    .expect("failed to call write function");
            }

            storage
                .update_metadata(
                    &metadata,
                    &offline_metadata,
                    resource,
                    data_len,
                    |storage, resource| mtime_of(resource, storage),
                )
                .await?;
            Ok(())
        }) as WriteFuture
    };

    Ok(Options::new(
        Box::new(read),
        Box::new(write),
        Box::new(move |resource, storage| {
            let metadata = Arc::clone(&delete_metadata);
            let offline_metadata = Arc::clone(&delete_offline_metadata);
            let delete_callback = delete_delete_callback.clone();
            Box::pin(async move {
                let path = path_from_storage(storage, &resource);
                debug!("Removing {resource} in {storage} from metadata cache.");
                storage
                    .delete_update_metadata(&metadata, &offline_metadata, &resource)
                    .await;
                let lock = delete_callback.lock().await;
                lock.call(&[path.into()])
                    .expect("failed to call delete callback");

                Ok(())
            }) as DeleteFuture
        }),
        Box::new(move || {
            let metadata = Arc::clone(&diff_metadata);
            let offline_metadata = Arc::clone(&diff_offline_metadata);
            let list_all = Arc::clone(&diff_list_all);
            let mtime_of = Arc::clone(&diff_mtime_of);
            Box::pin(async move {
                debug!("Getting diff");
                let current_metadata = metadata_new(Storage::Current, list_all(), &*mtime_of)
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

pub async fn connect_ws(url: &str) -> Result<Web, ApplicationError> {
    info!("Connecting to {url:?}.");
    let result = websocket::futures::WebSocket::open(url);
    let conenction = result.map_err(|err| ApplicationError::ConnectionFailed(err.to_string()))?;
    let (w, r) = conenction.split();
    Ok(Web(
        Arc::new(Mutex::new(WriteHalf(w))),
        Arc::new(Mutex::new(ReadHalf(r))),
    ))
}
async fn initial_metadata<
    F: Future<Output = Result<Metadata, io::Error>>,
    ReadF: Future<Output = Result<Option<Vec<u8>>, ()>>,
    Mtime,
    MtimeFuture: Future<Output = Option<Mtime>>,
    PopulatedFuture: Future<Output = bool>,
>(
    name: &str,
    new: impl Fn() -> F,
    force_pull: bool,
    read: impl Fn(String) -> ReadF,
    mtime: impl Fn(String) -> MtimeFuture,
    populated: PopulatedFuture,
) -> Result<Metadata, io::Error> {
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
                let meta = mtime(resource.to_owned()).await;
                if meta.is_none() {
                    differing.push(resource.to_owned());
                    error!("File {resource} is not found on disk.");
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

            let populated = populated.await;
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
