#![deny(
    clippy::pedantic,
    unreachable_pub,
    missing_debug_implementations,
    missing_docs
)]

use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct Capabilities {
    version: semver::Version,
    /// The client is striving to be persistent. These will regularly do [`Message::HashCheck`]
    persistent: bool,
    uuid: u64,
}

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct ModifyEvent {
    resource: String,
    section: Section,
}
impl ModifyEvent {
    /// Calculates the diff if `source` is [`Some`].
    pub fn new(resource: String, section: Section, source: Option<&[u8]>) -> Self {
        if source.is_some() {
            unimplemented!("Implement diff");
        }
        Self { resource, section }
    }
}
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct DeleteEvent;
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct MoveEvent;

macro_rules! into_event {
    ($type:ty, $enum_name:ident) => {
        impl<'a> From<$type> for Event {
            fn from(event: $type) -> Self {
                Self::$enum_name(event)
            }
        }
    };
}

into_event!(ModifyEvent, Modify);

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub enum Event {
    Modify(ModifyEvent),
    Delete(DeleteEvent),
    Move(MoveEvent),
}

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct Section {
    /// The start of the previous data in the resource.
    start: usize,
    /// The end of the previous data in the resource.
    end: usize,
    /// A reference to the data.
    data: Vec<u8>,
}
impl Section {
    pub fn new(start: usize, end: usize, data: Vec<u8>) -> Self {
        Self { start, end, data }
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct EventMessage {
    events: Vec<Event>,
    uuid: u64,
}
impl EventMessage {
    pub fn new(events: Vec<Event>) -> Self {
        Self::with_uuid(events, rand::random())
    }
    pub fn with_uuid(events: Vec<Event>, uuid: u64) -> Self {
        Self { events, uuid }
    }
    pub fn with_rng(events: Vec<Event>, rng: impl rand::Rng) -> Self {
        Self::with_uuid(events, rng.gen())
    }
    pub fn event_iter(&self) -> impl Iterator<Item = &Event> {
        self.events.iter()
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub enum Message {
    /// The client sending this is connecting to the network.
    ///
    /// Will declare it's capabilities.
    ///
    /// Expects a [`Self::Welcome`] or [`Self::InvalidUuid`].
    Hello(Capabilities),
    /// Response to the [`Self::Hello`] message.
    Welcome(Capabilities),
    /// The [`Message::Hello`] uses an occupied UUID.
    InvalidUuid,
    /// A client has new data to share.
    Event(EventMessage),
    /// A client tries to get the most recent data.
    ///
    /// You should respond with a [`Self::FastForwardReply`].
    FastForward,
    /// A reply to a [`Self::FastForward`] request.
    FastForwardReply,
    /// A request to get the diffs and sync the specified resources.
    Sync,
    /// The response with hashes of the specified resources.
    SyncReply,
    /// Requests all the hashes of all the resources specified in the regex.
    HashCheck,
    /// A reply with all the hashes of all the requested files.
    HashCheckReply,
}

pub struct Manager {
    rng: rand::rngs::ThreadRng,
}
impl Manager {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn process<'a>(&mut self, event: Event) -> EventMessage {
        EventMessage::with_rng(vec![event], &mut self.rng)
    }
}
impl Default for Manager {
    fn default() -> Self {
        Self {
            rng: rand::thread_rng(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn send_diff() {
        let mut manager = Manager::default();

        let event = ModifyEvent::new(
            "test.txt".into(),
            Section::new(0, 0, b"Some test data.".to_vec()),
            None,
        );

        let mut message = manager.process(event.clone().into());

        // The message are sent to a different client.

        match message {}

        let mut receiver = Manager::default();

        let mut events = actions
            .into_iter()
            .map(|action| receiver.apply(action))
            .flatten();

        assert_eq!(events.next(), Some(event));
        assert_eq!(events.next(), None);

        // receive action...
        // assert the only action is to modify `test.txt` with the same section as before.
    }

    // Test doing this ↑ but simplifying as it had previous data, stored how?

    // Test when the underlying data has changed without events; then this library is called again.
    // A special call to the library, which will request all the files, mtime & size to see which
    // have changed.
}
