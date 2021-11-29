use agde::*;
use std::time::{Duration, SystemTime};

fn manager() -> Manager {
    Manager::new(false, 0, Duration::from_secs(60), 200)
}

#[test]
fn send_diff() {
    let (message_bin, message_base64, sender_uuid): (Vec<u8>, String, Uuid) = {
        let mut manager = manager();

        let event = ModifyEvent::new(
            "test.txt".into(),
            vec![VecSection::whole_resource(0, b"Some test data.".to_vec())],
            None,
        );

        let message = manager.process_event(event);

        (message.bin(), message.base64(), manager.uuid())
    };

    // The message are sent to a different client.

    // receive message...
    let message = Message::from_bin(&message_bin).unwrap();
    let message_base64 = Message::from_base64(&message_base64).unwrap();
    assert_eq!(message, message_base64);
    assert_eq!(message.sender(), sender_uuid);

    let mut receiver = manager();

    match message.inner() {
        MessageKind::Event(event) => {
            // assert the event is to modify `test.txt` with the same section as before.
            assert_eq!(
                event.inner(),
                &EventKind::Modify(ModifyEvent::new(
                    "test.txt".into(),
                    vec![VecSection::whole_resource(0, b"Some test data.".to_vec())],
                    None
                ))
            );

            let event_applier = receiver
                .apply_event(event, message.uuid())
                .expect("Got event from future.");
            match event_applier.event().inner() {
                EventKind::Modify(ev) => {
                    assert_eq!(event_applier.resource(), Some("test.txt"));

                    let mut test = Vec::new();
                    let mut res = SliceBuf::with_whole(&mut test);
                    res.extend_to_needed(ev.sections(), b' ');

                    event_applier.apply(&mut res).expect("Buffer too small!");
                }
                _ => panic!("Wrong EventKind"),
            }
        }
        kind => {
            panic!("Got {:?}, but expected a Event!", kind);
        }
    }
}

#[test]
fn rework_history() {
    fn process_message(
        manager: &mut Manager,
        message: &Message,
        resource: &mut SliceBuf<&mut Vec<u8>>,
    ) {
        match message.inner() {
            MessageKind::Event(event) => {
                let event_applier = manager
                    .apply_event(event, message.uuid())
                    .expect("Got event from future.");
                match event_applier.event().inner() {
                    EventKind::Modify(ev) => {
                        assert_eq!(ev.resource(), "private/secret.txt");

                        resource.extend_to_needed(ev.sections(), b' ');

                        event_applier.apply(resource).expect("Buffer too small!");
                    }
                    _ => panic!("Wrong EventKind"),
                }
            }
            kind => {
                panic!("Got {:?}, but expected a Event!", kind);
            }
        }
    }
    let (first_message, second_message) = {
        let mut sender = manager();

        let first_event = ModifyEvent::new(
            "private/secret.txt".into(),
            vec![VecSection::new(0, 0, "Hello world!".into())],
            None,
        )
        .into();
        let second_event = ModifyEvent::new(
            "private/secret.txt".into(),
            vec![VecSection::new(6, 11, "friend".into())],
            None,
        )
        .into();
        (
            sender.process_event(Event::with_timestamp(
                first_event,
                sender.uuid(),
                SystemTime::now() - Duration::from_secs(10),
            )),
            sender.process_event(Event::with_timestamp(
                second_event,
                sender.uuid(),
                SystemTime::now(),
            )),
        )
    };

    let mut receiver = manager();

    let mut resource = Vec::new();
    let mut res = SliceBuf::with_whole(&mut resource);

    // Process second message first.
    process_message(&mut receiver, &second_message, &mut res);

    assert_eq!(res.filled(), b" ");
    let filled = res.filled().len();
    res.set_filled(12);
    assert_eq!(res.filled(), b"      friend");
    res.set_filled(filled);

    // Now, process first message.
    process_message(&mut receiver, &first_message, &mut res);

    assert_eq!(res.filled(), b"Hello friend!");
}

#[test]
fn basic_diff() {
    let mut resource =
        b"Some test data. Hope this test workes, as the whole diff algorithm is written by me!"
            .to_vec();
    let mut res = SliceBuf::with_whole(&mut resource);

    let mut mgr = manager();

    let event = ModifyEvent::new(
        "diff.bin".into(),
        vec![VecSection::whole_resource(
            res.filled().len(),
            b"Some test data. This test works, as the whole diff algorithm is written by me!"
                .to_vec(),
        )],
        Some(res.filled()),
    );

    let message = mgr.process_event(event);

    let mut receiver = manager();

    match message.inner() {
        MessageKind::Event(event) => {
            let event_applier = receiver
                .apply_event(event, message.uuid())
                .expect("Got event from future.");
            match event_applier.event().inner() {
                EventKind::Modify(ev) => {
                    res.extend_to_needed(ev.sections(), b' ');

                    event_applier.apply(&mut res).expect("Buffer too small!");
                }
                _ => panic!("Wrong EventKind"),
            }
        }
        kind => {
            panic!("Got {:?}, but expected a Event!", kind);
        }
    }
    let filled = res.filled().len();
    resource.truncate(filled);
    assert_eq!(
        String::from_utf8(resource).unwrap(),
        "Some test data. This test works, as the whole diff algorithm is written by me!"
    );
}

// Test when the underlying data has changed without events; then this library is called again.
// A special call to the library, which will request all the files, mtime & size to see which
// have changed.
