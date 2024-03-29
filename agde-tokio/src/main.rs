use agde::Manager;
use agde_tokio::Compression;
use clap::{command, Arg, Command};
use log::error;
use notify::Watcher;
use std::process;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

fn parse_validate<T: FromStr>(
    validate: impl Fn(&T) -> bool + Clone,
) -> impl Fn(&str) -> Result<T, String> + Clone
where
    <T as FromStr>::Err: ToString,
{
    move |s| {
        s.parse().map_err(|e: T::Err| e.to_string()).and_then(|v| {
            if validate(&v) {
                Ok(v)
            } else {
                Err("failed validation".to_owned())
            }
        })
    }
}

fn command() -> Command {
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
                .value_parser(parse_validate::<f64>(|f| *f > 0.)),
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
                .value_parser(parse_validate::<f64>(|f| *f > 0.)),
        )
        .arg(
            Arg::new("periodic")
                .short('p')
                .long("periodic-interval")
                .default_value("30")
                .help(
                    "Number of seconds (float) between periodic actions. \
                    The in-memory cache flushes each interval. \
                    Event log and hash checks are also sent according to this interval.",
                )
                .value_parser(parse_validate::<f64>(|f| *f > 0.)),
        )
        .arg(
            Arg::new("compress")
                .short('c')
                .long("compress")
                .value_parser(["none", "snappy", "zstd"])
                .default_value("zstd"),
        )
        .arg(Arg::new("server").long("server").help(
            "Run as a server. Disables the public storage \
            (reduces FS usage) and enables periodic hash checks.",
        ))
}

#[tokio::main]
async fn main() {
    env_logger::init();

    let matches = command().get_matches();

    let url = matches.get_one::<String>("url").unwrap();
    let force = matches.get_flag("force");

    let sync_interval = matches
        .get_one::<f64>("sync")
        .expect("--sync-interval takes a float value");
    let startup_duration = matches
        .get_one::<f64>("startup")
        .expect("--startup-duration takes a float value");
    let periodic_interval = matches
        .get_one::<f64>("periodic")
        .expect("--periodic-interval takes a float value");

    let compress = match matches
        .get_one::<String>("compress")
        .expect("we passed a default value")
        .as_str()
    {
        "none" => Compression::None,
        "snappy" => Compression::Snappy,
        "zstd" => Compression::Zstd,
        _ => unreachable!("we've covered all the possible values"),
    };
    let server = matches.get_flag("server");

    loop {
        let options = agde_tokio::options_fs(force, compress, "".into())
            .await
            .expect("failed to read file system metadata");
        let mut options = options
            .with_startup_duration(Duration::from_secs_f64(*startup_duration))
            .with_sync_interval(Duration::from_secs_f64(*sync_interval))
            .with_periodic_interval(Duration::from_secs_f64(*periodic_interval));

        if server {
            options = options.with_no_public_storage();
        }

        let options = options.arc();

        let log_lifetime = Duration::from_secs(60);

        let manager = Manager::new(server, 0, log_lifetime, 512);

        match agde_io::run(
            manager,
            options,
            || agde_tokio::connect_ws(url, None),
            |_msg| {},
            || {},
        )
        .await
        {
            Ok(handle) => {
                let watch_handle = Arc::new(handle.state().clone());
                let mut watcher = agde_tokio::watch_changes(move || {
                    let handle = Arc::clone(&watch_handle);
                    async move {
                        if let Err(err) = handle.commit_and_send(&mut []).await {
                            error!("Failed to commit and send: {err:?}")
                        };
                    }
                })
                .await;

                let watch_path = std::path::Path::new(".");
                let r = watcher.watch(watch_path, notify::RecursiveMode::Recursive);
                if r.is_err() {
                    error!("Failed to start listening. Falling back to commit interval.");
                }

                agde_tokio::catch_ctrlc(handle.state().clone()).await;

                let r = handle.wait().await;

                let _ = watcher.unwatch(watch_path);

                if let Err(err) = r {
                    error!("Got error when running: {err}. Trying to reconnect in 10s.");
                    tokio::time::sleep(Duration::from_secs(10)).await;
                } else {
                    process::exit(0)
                }
            }
            Err(err) => {
                error!("Got error: {err}. Trying to reconnect in 10s.");
                tokio::time::sleep(Duration::from_secs(10)).await;
            }
        }
    }
}
