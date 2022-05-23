use agde::Manager;
use log::error;
use notify::Watcher;
use std::process;
use std::time::Duration;

use agde_tokio::*;

#[tokio::main]
async fn main() {
    env_logger::init();

    // `TODO`: use clap for argument parsing.
    let url = "ws://localhost:8081/ws";

    loop {
        let options = native::options_fs(false)
            .await
            .expect("failed to read file system metadata");
        let options = options.with_startup_duration(Duration::from_secs(1)).arc();

        let log_lifetime = Duration::from_secs(60);

        if log_lifetime <= options.sync_interval() * 2 {
            error!("Increase frequency of sync or increase log lifetime.");
        }

        let manager = Manager::new(false, 0, log_lifetime, 512);

        match run(url, manager, options).await {
            Ok(handle) => {
                let mut watcher = native::watch_changes(handle.state().clone()).await;
                let watch_path = std::path::Path::new(".");
                let r = watcher.watch(watch_path, notify::RecursiveMode::Recursive);
                if r.is_err() {
                    error!("Failed to start listening. Falling back to commit interval.");
                }
                native::catch_ctrlc(handle.state().clone());
                handle.wait().await.unwrap();
                let _ = watcher.unwatch(watch_path);
                process::exit(0)
            }
            Err(err) => {
                error!("Got error: {err}. Trying to reconnect in 10s.");
                tokio::time::sleep(Duration::from_secs(10)).await;
            }
        }
    }
}
