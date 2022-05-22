use std::process;
use std::time::Duration;

use agde::Manager;
use log::error;

use agde_tokio::*;

#[tokio::main]
async fn main() {
    env_logger::init();

    // `TODO`: use clap for argument parsing.
    let url = "ws://localhost:8081/ws";

    loop {
        let mut options = native::options_fs(false)
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
            Ok(handle) => {
                native::catch_ctrlc(handle.state().clone());
                handle.wait().await.unwrap();
                process::exit(0)
            }
            Err(err) => {
                error!("Got error: {err}. Trying to reconnect in 10s.");
                tokio::time::sleep(Duration::from_secs(10)).await;
            }
        }
    }
}
