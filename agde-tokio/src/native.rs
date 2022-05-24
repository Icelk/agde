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

            let _ = send(&write, &manager.process_disconnect()).await;

            let clean = options.read_clean().await?;
            if clean.as_deref() != Some(b"y") {
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
                                        .write(resource, WriteStorage::current(), current, true)
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

pub async fn options_fs(force_pull: bool, compression: Compression) -> Result<Options, io::Error> {
    // empty temporary metadata
    let metadata = Arc::new(Mutex::new(Metadata::new(HashMap::new())));
    let offline_metadata = Arc::new(Mutex::new(Metadata::new(HashMap::new())));
    let write_metadata = Arc::clone(&metadata);
    let write_offline_metadata = Arc::clone(&offline_metadata);
    let delete_metadata = Arc::clone(&metadata);
    let delete_offline_metadata = Arc::clone(&offline_metadata);
    let diff_metadata = Arc::clone(&metadata);
    let diff_offline_metadata = Arc::clone(&offline_metadata);

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
                    println!("Decompress file {path:?}");
                    let buffer = file.fill_buf().map_err(|_| ())?;
                    println!(
                        "First 128 in buf {:?}",
                        String::from_utf8_lossy(&buffer[..buffer.len().min(128)])
                    );
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
                        Compression::Zstd => todo!(),
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
    let write = move |resource: String, storage, data: Vec<u8>| {
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
                    println!("Compresss! {file_path:?}");
                    match compression {
                        Compression::None => {
                            file.write_all(&data).map_err(|_| ())?;
                        }
                        Compression::Zstd => todo!(),
                        Compression::Snappy => {
                            let mut compressor = snap::write::FrameEncoder::new(&mut file);
                            compressor.write_all(&data).map_err(|_| ())?;
                        }
                    }
                } else {
                    println!("Dont");
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

    let meta = initial_metadata(
        "metadata",
        || metadata_new(Storage::Public),
        force_pull,
        |res| read(res, Storage::Meta),
        |res, data| write(res, WriteStorage::Meta, data),
    )
    .await?;
    // A metadata cache that is held constant between diff calls.
    let offline_meta = initial_metadata(
        "metadata-offline",
        || {
            let r = Ok(meta.clone());
            futures::future::ready(r)
        },
        force_pull,
        |res| read(res, Storage::Meta),
        |res, data| write(res, WriteStorage::Meta, data),
    )
    .await?;

    {
        *metadata.lock().await = meta;
        *offline_metadata.lock().await = offline_meta;
    }

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
                    debug!("Removing {resource} from metdata cache.");
                    metadata.remove(&resource);
                }
                {
                    let mut metadata = metadata.lock().await;
                    debug!("Removing {resource} from metadata cache.");
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

            if !populated || force_pull {
                let metadata = new().await?;
                let r = write(
                    name.to_owned(),
                    bincode::serde::encode_to_vec(
                        &metadata,
                        bincode::config::standard().write_fixed_array_length(),
                    )
                    .expect("failed to serialize metadata"),
                )
                .await;
                if let Err(err) = r {
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
