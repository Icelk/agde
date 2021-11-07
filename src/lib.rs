#![deny(
    clippy::pedantic,
    unreachable_pub,
    missing_debug_implementations,
    missing_docs
)]

pub enum Event {
    Modify(ModifyEvent),
    Delete(DeleteEvent),
    Move(MoveEvent),
}

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

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn send_diff() {
        let mut manager = Manager::default();

        let event = ModifyEvent::new("test.txt", Section::new(0, 0, b"Some test data."));

        let mut action_iter = manager.process(event.clone().into());

        let actions = action_iter.collect();

        // The actions are sent to a different client.

        let mut receiver = Manager::default();

        let mut events = actions.into_iter().map(|action| receiver.apply(action)).flatten();

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
