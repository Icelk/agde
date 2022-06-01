//! Periodic events.

use crate::*;

/// returns true every denominator time, on average
fn one_in(denominator: f64, rng: &mut impl rand::Rng) -> bool {
    let mut random: f64 = rng.gen();
    random *= denominator;
    random < 1.0
}
pub async fn start<P: Platform>(
    options: &Options<P>,
    mgr: &Arc<Mutex<Manager>>,
    platform: &PlatformExt<P>,
) {
    // start at 1 so no matches below don't trigger at the first iteration
    let mut i: u64 = 1;
    loop {
        // flushing
        {
            flush(options, i % 10 == 0).await;
        }
        // event log
        if i % 10 == 0 {
            event_log_check(mgr, platform).await;
        }
        // hash check
        if i % 30 == 0 {
            hash_check(mgr, platform).await;
        }

        P::Rt::sleep(options.periodic_interval).await;
        i += 1;
    }
}
async fn flush<P: Platform>(options: &Options<P>, clear: bool) {
    let r = if clear {
        options.flush_out().await
    } else {
        options.flush().await
    };
    if let Err(err) = r {
        error!("Error while flushing data: {err:?}");
    };
}
pub async fn event_log_check(mgr: &Arc<Mutex<Manager>>, platform: &PlatformExt<impl Platform>) {
    let mut manager = mgr.lock().await;
    manager.clean_log_checks();

    if !manager.is_fast_forwarding()
        && manager.pier_count() > 0
        && one_in(manager.client_count() as f64, &mut *manager.rng())
    {
        let count = manager.event_log_limit() / 4;
        let msg = manager.process_event_log_check(count);
        drop(manager);
        if let Some((msg, conversation_uuid)) = msg {
            if let Err(err) = platform.send(&msg).await {
                error!("Error when trying to send event log check message: {err:?}");
            } else {
                let mgr = Arc::clone(mgr);
                assure_event_log_check(mgr, platform.clone(), conversation_uuid).await;
            }
        }
    }
}
pub async fn assure_event_log_check<P: Platform>(
    mgr: Arc<Mutex<Manager>>,
    platform: PlatformExt<P>,
    conversation_uuid: agde::Uuid,
) {
    P::Rt::spawn(async move {
        P::Rt::sleep(Duration::from_secs(8)).await;
        let mut manager = mgr.lock().await;
        if let Some(pier) = manager.assure_log_check(conversation_uuid) {
            let hc = manager.process_hash_check(pier).expect(
                "BUG: Internal agde state error, trying to send a \
            hash check after a event log check while fast forwarding.",
            );
            drop(manager);
            if let Err(err) = platform.send(&hc).await {
                error!("Error when trying to send hash check message: {err:?}");
            } else {
                // check that the pier responded.
                P::Rt::spawn(async move {
                    let pier = pier.uuid();
                    watch_hash_check(pier, &mgr, &platform).await;
                });
            }
        }
    });
}
async fn hash_check<P: Platform>(mgr: &Arc<Mutex<Manager>>, platform: &PlatformExt<P>) {
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
            info!("Sending hash check to {}", pier.uuid());
            let msg = manager
                .process_hash_check(pier)
                .expect("BUG: Internal agde state error when trying to send a hash check");
            drop(manager);
            if let Err(err) = platform.send(&msg).await {
                error!("Error when trying to send hash check message: {err:?}");
            } else {
                let mgr = Arc::clone(mgr);
                let platform = platform.clone();
                // check that the pier responded.
                P::Rt::spawn(async move {
                    let pier = pier.uuid();
                    watch_hash_check(pier, &mgr, &platform).await;
                });
            }
        }
    }
}
async fn watch_hash_check<P: Platform>(
    mut pier: agde::Uuid,
    mgr: &Mutex<Manager>,
    platform: &PlatformExt<P>,
) {
    loop {
        P::Rt::sleep(Duration::from_secs(30)).await;
        let mut manager = mgr.lock().await;

        // we're still hash checking with the same pier
        if manager.hash_checking() == Some(pier) {
            info!("Hash check with {pier} failed. Retrying.");
            let action = manager.apply_cancelled(pier);

            if let agde::CancelAction::HashCheck(hc_pier) = action {
                let hc = manager.process_hash_check(hc_pier).expect(
                    "BUG: Internal agde state error when recovering from a failed hash check",
                );

                drop(manager);

                pier = hc_pier.uuid();

                if let Err(err) = platform.send(&hc).await {
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
