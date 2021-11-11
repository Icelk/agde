#![deny(
    clippy::pedantic,
    unreachable_pub,
    missing_debug_implementations,
    missing_docs
)]

use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct ModifyEvent<'a> {
    resource: &'a str,
    section: Section<'a>,
    source: Option<&'a [u8]>,
}
impl<'a> ModifyEvent<'a> {
    pub fn new(resource: &'a str, section: Section<'a>, source: Option<&'a [u8]>) -> Self {
        Self {
            resource,
            section,
            source,
        }
    }
}

macro_rules! into_event {
    ($type:ty, $enum_name:ident) => {
        impl<'a> From<$type> for Event<'a> {
            fn from(event: $type) -> Self {
                Self::$enum_name(event)
            }
        }
    };
}

into_event!(ModifyEvent<'a>, Modify);

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub enum Event<'a> {
    Modify(ModifyEvent<'a>),
    Delete(DeleteEvent),
    Move(MoveEvent),
}

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct Section<'a> {
    /// The start of the previous data in the resource.
    start: usize,
    /// The end of the previous data in the resource.
    end: usize,
    /// A reference to the data.
    data: &'a [u8],
}
impl<'a> Section<'a> {
    pub fn new(start: usize, end: usize, data: &'a [u8]) -> Self {
        Self { start, end, data }
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct Message<'a> {
    events: Vec<Event<'a>>,
    uuid: u64,
}
impl<'a> Message<'a> {
    pub fn new(events: Vec<Event<'a>>) -> Self {
        Self::with_uuid(events, rand::random())
    }
    pub fn with_uuid(events: Vec<Event<'a>>, uuid: u64) -> Self {
        Self { events, uuid }
    }
    pub fn with_rng(events: Vec<Event<'a>>, rng: impl rand::Rng) -> Self {
        Self::with_uuid(events, rng.gen())
    }
    pub fn event_iter(&self) -> impl Iterator<Item = &Event> {
        self.events.iter()
    }
}

pub struct Manager {
    rng: rand::rngs::ThreadRng,
}
impl Manager {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn process<'a>(&mut self, event: Event<'a>) -> Message<'a> {
        Message::with_rng(vec![event], &mut self.rng)
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

        let event = ModifyEvent::new("test.txt", Section::new(0, 0, b"Some test data."), None);

        let mut message = manager.process(event.clone().into());

        let actions: Vec<_> = message.event_iter().collect();

        // The actions are sent to a different client.

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
