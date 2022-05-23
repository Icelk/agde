//! Code for the native (tokio+fs) implementation of agde.
//!
//! Already keeping this separate from the `agde-tokio` lib to make it easier to adapt this to the
//! web.

use crate::*;

// OK to have lazy_static here since our native implementation only has one agde instance running.
lazy_static::lazy_static! {
    static ref STATE: std::sync::Mutex<Option<StateHandle>> = std::sync::Mutex::new(None);
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

pub fn catch_ctrlc(handle: StateHandle) {
    let manager = Arc::clone(&handle.manager);
    let options = Arc::clone(&handle.options);
    let write = Arc::clone(&handle.write);

    let handler = ctrlc::set_handler(move || {
        info!("Caught ctrlc");
        let manager = Arc::clone(&manager);
        let options = Arc::clone(&options);
        let write = Arc::clone(&write);

        let runtime = tokio::runtime::Builder::new_current_thread()
            .max_blocking_threads(1)
            .build()
            .expect("failed to start tokio when handling ctrlc");
        let returned = runtime.block_on(async move {
            let mut manager = manager.lock().await;

            let _ = send(&write, manager.process_disconnect()).await;

            let clean = options.read_clean().await?;
            if clean.as_deref() == Some(b"y") {
                info!("State clean. Exiting.");
                return Ok(());
            }
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
                                    .write(resource, WriteStorage::current(), current)
                                    .await?;
                            }
                        }
                    }
                }
            }

            // we are clean again!
            options.write_clean("y").await?;

            Ok::<(), ApplicationError>(())
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
            if lock.is_none() {
                warn!("Failed to set ctrlc handler: another handler has been set");
            } else {
                *lock = Some(handle);
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

pub async fn options_fs(force_pull: bool) -> Result<Options, io::Error> {
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
    let sync_metadata = Arc::clone(&metadata);
    let sync_offline_metadata = Arc::clone(&offline_metadata);
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
        write: Box::new(move |resource, storage, data| {
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
                info!("Writing to {path}, {:?}", String::from_utf8_lossy(&data));

                let mut file = tokio::fs::File::create(&path).await.map_err(|_| ())?;
                file.write_all(&data).await.map_err(|_| ())?;
                file.flush().await.map_err(|_| ())?;
                match storage {
                    // meta storage obviously doesn't affect the files' metadata.
                    WriteStorage::Meta => {}
                    WriteStorage::Public(write_mtime, mut event_mtime) => {
                        let mut metadata = metadata.lock().await;

                        let mtime = match write_mtime {
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
                        if let Some(meta) = metadata.get(&resource) {
                            if meta.mtime_of_last_event() != SystemTime::UNIX_EPOCH {
                                event_mtime = meta.mtime_of_last_event();
                            }
                        }
                        let meta =
                            ResourceMeta::new_from_event(mtime, event_mtime, data.len() as u64);
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
                                    ResourceMeta::new(Some(mtime), data.len() as u64),
                                );
                            }
                            {
                                let mut metadata = metadata.lock().await;

                                let mut event_mtime = SystemTime::UNIX_EPOCH;
                                println!("Resetting mtime of public from current: {metadata:?}");
                                if let Some(meta) = metadata.get(&resource) {
                                    if meta.mtime_of_last_event() != SystemTime::UNIX_EPOCH {
                                        event_mtime = meta.mtime_of_last_event();
                                    }
                                }
                                let meta = ResourceMeta::new_from_event(
                                    Some(mtime),
                                    event_mtime,
                                    data.len() as u64,
                                );
                                metadata.insert(resource, meta);
                                println!("Resetting mtime of public from current: {metadata:?}");
                                metadata_sync(&*metadata, "metadata")
                                    .await
                                    .map_err(|_| ())?;
                            }
                        }
                    },
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
                let current_metadata = metadata_new(Storage::Current).await.map_err(|_| ())?;
                let mut offline_metadata = offline_metadata.lock().await;
                let changed = offline_metadata.changes(&current_metadata, true);
                let mut metadata = metadata.lock().await;
                metadata.apply_changes(&changed, &current_metadata);
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
        sync_metadata: Box::new(move |storage| {
            let metadata = match storage {
                Storage::Current => Some((Arc::clone(&sync_offline_metadata), "metadata")),
                Storage::Public => Some((Arc::clone(&sync_metadata), "metadata-offline")),
                Storage::Meta => None,
            };
            Box::pin(async move {
                if let Some((meta, name)) = metadata {
                    metadata_sync(&*meta.lock().await, name)
                        .await
                        .map_err(|_| ())
                } else {
                    Ok(())
                }
            })
        }),
        metadata,
        offline_metadata,
        startup_timeout: Duration::from_secs(7),
        sync_interval: Duration::from_secs(10),
        force_pull,
        verify_diffs: true,
    })
}

/// Watch for changes.
///
/// You have to call [`notify::Watcher::watch`] to start watching and
/// [`notify::Watcher::unwatch`] to stop the execution.
pub async fn watch_changes(handle: StateHandle) -> impl notify::Watcher {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let watcher =
        notify::recommended_watcher(move |res| tx.send(res).expect("receiving end failed"))
            .expect("failed to start watcher");
    let in_timeout = Arc::new(std::sync::Mutex::new(false));
    let handle = Arc::new(handle);
    tokio::spawn(async move {
        while let Some(ev) = rx.recv().await {
            let ev = match ev {
                Ok(ev) => ev,
                Err(err) => {
                    warn!("Watcher failed: {err:?}");
                    continue;
                }
            };
            // if
            if ev.paths.first().map_or(false, |path| {
                path.components()
                    .any(|component| component.as_os_str() == ".agde")
            }) {
                continue;
            }
            // debounce
            if !{ *in_timeout.lock().unwrap() } {
                *in_timeout.lock().unwrap() = true;
                let handle = Arc::clone(&handle);
                let in_timeout = Arc::clone(&in_timeout);
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_millis(100)).await;

                    if let Err(err) = handle.commit_and_send().await {
                        error!("Failed to commit and send: {err:?}")
                    };
                    *in_timeout.lock().unwrap() = false;
                });
            }
        }
        // just end the task
    });
    watcher
}

pub async fn connect_ws(url: &str) -> Result<(WriteHalf, ReadHalf), DynError> {
    info!("Connecting to {url:?}.");
    let result = tokio_tungstenite::connect_async(url).await;
    let conenction = result?;
    Ok(conenction.0.split())
}
pub async fn initial_metadata<F: Future<Output = Result<Metadata, io::Error>>>(
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
