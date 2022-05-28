//! Periodic events.

use crate::*;

/// returns true every denominator time, on average
fn one_in(denominator: f64, rng: &mut impl rand::Rng) -> bool {
    let mut random: f64 = rng.gen();
    random *= denominator;
    random < 1.0
}
pub async fn start(options: &Options, mgr: &Arc<Mutex<Manager>>, write: &Arc<Mutex<WriteHalf>>) {
    // start at 1 so no matches below don't trigger at the first iteration
    let mut i: u64 = 1;
    loop {
        // flushing
        {
            flush(options, i % 10 == 0).await;
        }
        // event log
        if i % 10 == 0 {
            event_log_check(mgr, write).await;
        }
        // hash check
        if i % 60 == 0 {
            hash_check(mgr, write).await;
        }

        tokio::time::sleep(options.periodic_interval).await;
        i += 1;
    }
}
async fn flush(options: &Options, clear: bool) {
    let r = if clear {
        options.flush_out().await
    } else {
        options.flush().await
    };
    if let Err(err) = r {
        error!("Error while flushing data: {err:?}");
    };
}
async fn event_log_check(mgr: &Arc<Mutex<Manager>>, write: &Arc<Mutex<WriteHalf>>) {
    let mut manager = mgr.lock().await;
    if !manager.is_fast_forwarding()
        && manager.pier_count() > 0
        && one_in(manager.client_count() as f64, &mut *manager.rng())
    {
        manager.clean_event_uuid_log_checks();
        let count = manager.event_log_limit() / 4;
        let msg = manager.process_event_log_check(count);
        drop(manager);
        if let Some((msg, conversation_uuid)) = msg {
            if let Err(err) = send(write, &msg).await {
                error!("Error when trying to send event log check message: {err:?}");
            } else {
                let mgr = Arc::clone(mgr);
                let write = Arc::clone(write);
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_secs(8)).await;

                    let mut manager = mgr.lock().await;
                    if let Some(pier) = manager.assure_event_uuid_log(conversation_uuid) {
                        let hc = manager.process_hash_check(pier).expect(
                            "BUG: Internal agde state error, trying to send a \
                            hash check after a event log check while fast forwarding.",
                        );
                        drop(manager);
                        if let Err(err) = send(&write, &hc).await {
                            error!("Error when trying to send hash check message: {err:?}");
                        } else {
                            // check that the pier responded.
                            tokio::spawn(async move {
                                let pier = pier.uuid();
                                watch_hash_check(pier, &mgr, &write).await;
                            });
                        }
                    }
                });
            }
        }
    }
}
async fn hash_check(mgr: &Arc<Mutex<Manager>>, write: &Arc<Mutex<WriteHalf>>) {
    let mut manager = mgr.lock().await;
    let other_persistent = manager
        .filter_piers(|_, capabilities| capabilities.persistent())
        .count();
    if !manager.is_fast_forwarding()
        && manager.capabilities().persistent()
        && other_persistent > 0
        && one_in((other_persistent + 1) as f64, &mut *manager.rng())
    {
        let pier = manager.hash_check_persistent();
        if let Some(pier) = pier {
            let msg = manager
                .process_hash_check(pier)
                .expect("BUG: Internal agde state error when trying to send a hash check");
            drop(manager);
            if let Err(err) = send(write, &msg).await {
                error!("Error when trying to send hash check message: {err:?}");
            } else {
                let mgr = Arc::clone(mgr);
                let write = Arc::clone(write);
                // check that the pier responded.
                tokio::spawn(async move {
                    let pier = pier.uuid();
                    watch_hash_check(pier, &mgr, &write).await;
                });
            }
        }
    }
}
async fn watch_hash_check(mut pier: agde::Uuid, mgr: &Mutex<Manager>, write: &Mutex<WriteHalf>) {
    loop {
        tokio::time::sleep(Duration::from_secs(30)).await;
        let mut manager = mgr.lock().await;

        // we're still hash checking with the same pier
        if manager.hash_checking() == Some(pier) {
            let action = manager.apply_cancelled(pier);

            if let agde::CancelAction::HashCheck(hc_pier) = action {
                let hc = manager.process_hash_check(hc_pier).expect(
                    "BUG: Internal agde state error when recovering from a failed hash check",
                );

                drop(manager);

                pier = hc_pier.uuid();

                if let Err(err) = send(write, &hc).await {
                    error!(
                        "Error when trying to fast forward to \
                        other piers after one failed: {err:?}"
                    );
                };
                continue;
            }
        }
        break;
    }
}
