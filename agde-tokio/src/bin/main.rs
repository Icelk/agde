use agde::Manager;
use clap::{command, Arg, Command};
use log::error;
use notify::Watcher;
use std::process;
use std::str::FromStr;
use std::time::Duration;

use agde_tokio::*;

fn validate<T: FromStr>(validate: impl Fn(T) -> bool) -> impl Fn(&str) -> Result<(), String> {
    move |v| {
        if let Ok(v) = v.parse::<T>() {
            if validate(v) {
                Ok(())
            } else {
                Err("Validation failed.".into())
            }
        } else {
            Err(format!(
                "Failed to parse {v:?} into type {}",
                std::any::type_name::<T>()
            ))
        }
    }
}

fn command() -> Command<'static> {
    let command = command!();
    command
        .arg(
            Arg::new("url")
                .short('u')
                .long("url")
                .default_value("ws://localhost:8081/ws")
                .help("The WebSocket server to connect to. Requires the 'ws:' scheme."),
        )
        .arg(
            Arg::new("force")
                .short('f')
                .long("force")
                .help("Force overriding of local files if that's required."),
        )
        .arg(
            Arg::new("sync")
                .short('s')
                .long("sync-interval")
                .default_value("10")
                .help("Number of seconds (float) to wait between committing and syncing.")
                .validator(validate(|f: f64| f > 0.)),
        )
        .arg(
            Arg::new("startup")
                .short('w')
                .long("startup-duration")
                .default_value("1")
                .help(
                    "Number of seconds (float) to wait for piers \
                    to welcome before fast forwarding.",
                )
                .validator(validate(|f: f64| f > 0.)),
        )
        .arg(
            Arg::new("flush")
                .long("flush-interval")
                .default_value("30")
                .help("Number of seconds (float) between writing cache to the FS.")
                .validator(validate(|f: f64| f > 0.)),
        )
}

#[tokio::main]
async fn main() {
    env_logger::init();

    let matches = command().get_matches();

    let url = matches.value_of("url").unwrap();
    let force = matches.is_present("force");

    let sync_interval = matches
        .value_of_t("sync")
        .expect("--sync-interval takes a float value");
    let startup_duration = matches
        .value_of_t("startup")
        .expect("--startup-duration takes a float value");
    let flush_interval = matches
        .value_of_t("flush")
        .expect("--flush-interval takes a float value");

    loop {
        let options = native::options_fs(force)
            .await
            .expect("failed to read file system metadata");
        let options = options
            .with_startup_duration(Duration::from_secs_f64(startup_duration))
            .with_sync_interval(Duration::from_secs_f64(sync_interval))
            .with_flush_interval(Duration::from_secs_f64(flush_interval))
            .arc();

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
