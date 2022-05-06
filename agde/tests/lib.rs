use agde::*;
use std::time::{Duration, SystemTime};

fn manager() -> Manager {
    Manager::new(false, 0, Duration::from_secs(60), 512)
}

#[test]
fn send_diff() {
    let (message_bin, message_base64, sender_uuid): (Vec<u8>, String, Uuid) = {
        let mut manager = manager();

        let event = event::Modify::new("test.txt".into(), b"Some test data.", b"");

        let message = manager.process_event(event);

        (message.to_bin(), message.to_base64(), manager.uuid())
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
                &EventKind::Modify(event::Modify::new(
                    "test.txt".into(),
                    b"Some test data.",
                    b"",
                ))
            );

            let mut event_applier = receiver
                .apply_event(event, message.uuid())
                .expect("Got event from future.");
            match event_applier.event().inner() {
                EventKind::Modify(_ev) => {
                    assert_eq!(event_applier.resource(), Some("test.txt"));

                    // let mut res = SliceBuf::with_whole(&mut test);
                    // res.extend_to_needed(ev.data(), b' ');

                    event_applier.apply(&[]).expect("Buffer too small!");
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
        // resource: &mut SliceBuf<&mut Vec<u8>>,
        resource: &[u8],
    ) -> Vec<u8> {
        match message.inner() {
            MessageKind::Event(event) => {
                let mut event_applier = manager
                    .apply_event(event, message.uuid())
                    .expect("Got event from future.");
                match event_applier.event().inner() {
                    EventKind::Modify(ev) => {
                        assert_eq!(ev.resource(), "private/secret.txt");

                        event_applier.apply(resource).expect("Buffer too small!")
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

        let first_event =
            event::Modify::new("private/secret.txt".into(), "Hello world!".as_bytes(), b"").into();
        let second_event = event::Modify::new(
            "private/secret.txt".into(),
            "Hello friend!".as_bytes(),
            "Hello world!".as_bytes(),
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

    // Process second message first.
    resource = process_message(&mut receiver, &second_message, &resource);

    assert_eq!(resource, b"Hello friend!");

    // Now, process first message.
    resource = process_message(&mut receiver, &first_message, &resource);

    assert_eq!(resource, b"Hello friend!");
}

#[test]
fn basic_diff() {
    let mut resource =
        b"Some test data. Hope this test workes, as the whole diff algorithm is written by me!"
            .to_vec();

    let mut mgr = manager();

    let event = event::Modify::new(
        "diff.bin".into(),
        b"Some test data. This test works, as the whole diff algorithm is written by me!",
        &resource,
    );

    println!("Event: {event:?}");

    let message = mgr.process_event(event);

    let mut receiver = manager();

    match message.inner() {
        MessageKind::Event(event) => {
            let mut event_applier = receiver
                .apply_event(event, message.uuid())
                .expect("Got event from future.");
            match event_applier.event().inner() {
                EventKind::Modify(_ev) => {
                    // res.extend_to_needed(ev.data(), b' ');

                    println!("Resource: {:?}", std::str::from_utf8(&resource));
                    resource = event_applier.apply(&resource).expect("Buffer too small!");
                }
                _ => panic!("Wrong EventKind"),
            }
        }
        kind => {
            panic!("Got {:?}, but expected a Event!", kind);
        }
    }
    assert_eq!(
        String::from_utf8(resource).unwrap(),
        "Some test data. This test works, as the whole diff algorithm is written by me!"
    );
}

#[test]
fn apply_diff() {
    let (old, new) = {
        (
            b"\
hi

well ok
no

Got binary message.
Got text message: 'HI!'
Got binary message.
Got binary message.
Got binary message.
Got binary message.
Got text message: 'HI!'
Got binary message.
Got binary message.
Got binary message.
Got text message: 'HI!'
Got binary message.
Got text message: 'HI!'
Got binary message.
Got binary message.
Got binary message.
Got text message: 'HI!'
Got binary message.
Got text message: 'HI!'
Got binary message.
Got binary message.
Got binary message.
Got text message: 'HI!'
Got binary message.
Got text message: 'HI!'
Got binary message.
Got binary message.
Got text message: 'HI!'
Got binary message.
Got binary message.
Got binary message.
Got binary message.
Got text message: 'HI!'
Got binary message.
Got text message: 'HI!'
Got binary message.
Got binary message.
Got binary message.
Got binary message.

wowsies!",
            b"\
hi

well ok
no

Got binary message.
Got text message: 'HI!'
Got binary message.
Got binary message.
Got binary message.
Got binary message.
Got text message: 'HI!'
Got binary message.
Got binary message.
Got binary message.
Got text message: 'HI!'
Got binary message.
Got text message: 'HI!'
Got binary message.
Got binary message.
Got binary message.
Got text message: 'HI!'
Got binary message.
Got text message: 'HI!'
Got binary message.
Got binary message.
Got binary message.
Got text message: 'HI!'
Got binary message.
Got text message: 'HI!'
Got binary message.
Got binary message.
Got text message: 'HI!'
Got binary message.
Got binary message.
Got binary message.
Got binary message.
Got text message: 'HI!'
Got binary message.
Got text message: 'HI!'
Got binary message.
Got binary message.
Got binary message.
Got binary message.

wowsies!
ok",
        )
    };

    let mut old = old.to_vec();
    let new_vec = new.to_vec();

    let mut mgr = manager();

    let event = event::Modify::new("diff.bin".into(), &new_vec, &old);

    let message = mgr.process_event(event);

    let mut receiver = manager();

    match message.inner() {
        MessageKind::Event(event) => {
            let mut event_applier = receiver
                .apply_event(event, message.uuid())
                .expect("Got event from future.");
            match event_applier.event().inner() {
                EventKind::Modify(_ev) => {
                    // res.extend_to_needed(ev.data(), 0);

                    old = event_applier.apply(&old).expect("Buffer too small!");
                }
                _ => panic!("Wrong EventKind"),
            }
        }
        kind => {
            panic!("Got {:?}, but expected a Event!", kind);
        }
    }
    assert_eq!(
        std::str::from_utf8(&old).unwrap(),
        std::str::from_utf8(new).unwrap(),
        "differing. Old (adjusted):\n{}\n\nNew (target):\n{}",
        std::str::from_utf8(&old).unwrap(),
        std::str::from_utf8(new).unwrap(),
    );
}

// Test when the underlying data has changed without events; then this library is called again.
// A special call to the library, which will request all the files, mtime & size to see which
// have changed.
