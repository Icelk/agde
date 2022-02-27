//! Modification of resources through sections.
//!
//! See [`Section`] for more details.

use std::{cmp, isize};

use serde::{Deserialize, Serialize};

/// A buffer containing a byte slice and a length of the filled data.
#[derive(Debug)]
#[must_use]
pub struct SliceBuf<T: AsMut<[u8]>> {
    slice: T,
    len: usize,
}
impl<T: AsMut<[u8]>> SliceBuf<T> {
    /// Creates a new buffer with length set to `0`.
    /// Don't forget to [`Self::advance`] to set the filled region of `slice`.
    /// If you forget to do this, new data will be overridden at the start.
    #[allow(clippy::inline_always)]
    #[inline(always)]
    pub fn new(slice: T) -> Self {
        Self { slice, len: 0 }
    }
    /// Creates a new buffer with the fill level set to `filled`.
    ///
    /// # Panics
    ///
    /// Will panic if `filled > slice.len()`.
    #[allow(clippy::inline_always)]
    #[inline(always)]
    pub fn with_filled(slice: T, filled: usize) -> Self
    where
        T: AsRef<[u8]>,
    {
        let mut me = Self::new(slice);
        me.set_filled(filled);
        me
    }
    /// Creates a new buffer with the fill level being the length of `slice`.
    /// This assumes the whole buffer is filled with initialized data.
    #[allow(clippy::inline_always)]
    #[inline(always)]
    pub fn with_whole(slice: T) -> Self
    where
        T: AsRef<[u8]>,
    {
        let len = slice.as_ref().len();
        Self::with_filled(slice, len)
    }
    pub(crate) fn slice(&self) -> &[u8]
    where
        T: AsRef<[u8]>,
    {
        self.slice.as_ref()
    }
    pub(crate) fn slice_mut(&mut self) -> &mut [u8] {
        self.slice.as_mut()
    }
    /// Sets the size of the filled region of the buffer.
    ///
    /// # Panics
    ///
    /// Panics if `n > self.capacity()`.
    #[allow(clippy::inline_always)]
    #[inline(always)]
    pub fn set_filled(&mut self, n: usize)
    where
        T: AsRef<[u8]>,
    {
        assert!(
            n <= self.slice().len(),
            "Tried to set filled to {} while length is {}",
            n,
            self.slice().len()
        );
        self.len = n;
    }
    /// Advances the size of the filled region of the buffer.
    ///
    /// # Panics
    ///
    /// Panics if `self.len() + n > self.capacity()`.
    #[allow(clippy::inline_always)]
    #[inline(always)]
    pub fn advance(&mut self, n: usize)
    where
        T: AsRef<[u8]>,
    {
        self.set_filled(self.len + n);
    }
    /// Get the filled region of the buffer.
    #[must_use]
    #[allow(clippy::inline_always)]
    #[inline(always)]
    pub fn filled(&self) -> &[u8]
    where
        T: AsRef<[u8]>,
    {
        &self.slice()[..self.len]
    }
    /// Get a mutable reference of the filled region of the buffer.
    #[must_use]
    #[allow(clippy::inline_always)]
    #[inline(always)]
    pub fn filled_mut(&mut self) -> &mut [u8]
    where
        T: AsRef<[u8]>,
    {
        let len = self.len;
        &mut self.slice_mut()[..len]
    }
    /// Size of the capacity of the buffer. This cannot be increased.
    #[must_use]
    #[allow(clippy::inline_always)]
    #[inline(always)]
    pub fn capacity(&self) -> usize
    where
        T: AsRef<[u8]>,
    {
        self.slice().len()
    }
}
impl SliceBuf<&mut Vec<u8>> {
    /// Extends this [`SliceBuf`] to fit `sections`.
    ///
    /// This only works if the internal type is a mutable reference to a [`Vec`].
    pub fn extend_to_needed<'b, T: Iterator<Item = &'b S>, S: Section + 'b>(
        &mut self,
        sections: impl IntoIterator<Item = &'b S, IntoIter = T>,
        fill: u8,
    ) {
        Section::apply_len(sections.into_iter(), self.slice, self.filled().len(), fill);
    }
}

/// An error during [`DataSection::apply`] and [`crate::log::EventApplier::apply`].
#[derive(Debug)]
pub enum ApplyError {
    /// [`SliceBuf::capacity`] is too small.
    BufTooSmall,
    /// The function called must not be called on the current event.
    InvalidEvent,
}

/// Adds `b` to `a`. There will be no loss in range.
///
/// # Errors
///
/// Returns an error if `a + b` is negative.
#[allow(clippy::inline_always)]
#[inline(always)]
pub(crate) fn add_iusize(a: usize, b: isize) -> Result<usize, ()> {
    // We've checked that with the if else.
    #[allow(clippy::cast_sign_loss)]
    let result = if b < 0 {
        let b = (-b) as usize;
        if b > a {
            return Err(());
        }
        a - b
    } else {
        a + (b as usize)
    };
    Ok(result)
}

/// [`Section::end`] must always be after [`Section::start`].
pub trait Section {
    /// The start of the section to replace in the resource.
    fn start(&self) -> usize;
    /// The end of the section to replace in the resource.
    fn end(&self) -> usize;
    /// Length of new data to fill between [`Section::start`] and [`Section::end`].
    fn new_len(&self) -> usize;
    /// Length of the data between [`Section::start`] and [`Section::end`].
    ///
    /// # Panics
    ///
    /// Panics if the end is before the start.
    /// This guarantee should be upheld by the implementer of [`Section`].
    #[allow(clippy::inline_always)]
    #[inline(always)]
    fn old_len(&self) -> usize {
        self.end() - self.start()
    }
    /// Difference between old and new length of resource.
    ///
    /// # Panics
    ///
    /// Panics if the end is before the start.
    /// This guarantee should be upheld by the implementer of [`Section`].
    ///
    /// Will also panic if any of the lengths don't fit in a [`isize`].
    #[allow(clippy::inline_always)]
    #[inline(always)]
    fn len_difference(&self) -> isize {
        isize::try_from(self.new_len()).expect("length too large for isize.")
            - isize::try_from(self.old_len()).expect("length too large for isize.")
    }
    /// The needed length of the [`SliceBuf`].
    /// Should be set to this before calling [`crate::log::EventApplier::apply`].
    ///
    /// May return a value lower than `resource_len`. This should not be applied until after the
    /// application.
    fn needed_len(&self, resource_len: usize) -> usize {
        let diff = add_iusize(resource_len, self.len_difference()).unwrap_or(0) + 1;
        let end = cmp::max(self.end() + 1, self.start() + self.new_len() + 1);
        cmp::max(diff, end)
    }
    /// Extends the `buffer` with `fill` to fit the sections.
    /// `len` is the length of the [`SliceBuf::filled`] part.
    ///
    /// # Panics
    ///
    /// Will panic if `len` is greater than half the memory size.
    /// This is 2GiB on 32-bit systems and stupidly large on 64-bit systems.
    fn apply_len<'a>(me: impl Iterator<Item = &'a Self>, buffer: &mut Vec<u8>, len: usize, fill: u8)
    where
        Self: 'a,
    {
        let mut current_size = isize::try_from(len).expect(
            "usize can't fit in isize. Use resource lengths less than 1/2 the memory size.",
        );
        let mut max = current_size;
        // let mut end = 0;
        for me in me {
            current_size += me.len_difference();
            max = max
                .max(current_size)
                .max(isize::try_from(me.end() + me.new_len()).unwrap_or(isize::MAX));
            // end = cmp::max(end, me.end() + 1);
            // end = cmp::max(end, me.start() + me.new_len() + 1);
        }
        // let needed = cmp::max(add_iusize(0, current_size).unwrap_or(0), end);
        // let additional = cmp::max(needed, buffer.len());
        println!("  NEEDED LENGTH {max}");
        // since max will always be > `len`, this is OK.
        #[allow(clippy::cast_sign_loss)]
        let max = max as usize;
        buffer.resize(max + 1, fill);
    }
    /// Extends the `buffer` with `fill` to fit this section.
    /// `len` is the length of the [`SliceBuf::filled`] part.
    fn apply_len_single(&self, buffer: &mut Vec<u8>, len: usize, fill: u8) {
        let needed = cmp::max(self.needed_len(len), len);
        buffer.resize(needed, fill);
    }
}
/// [`DataSection::end`] must always be after [`DataSection::start`].
#[allow(clippy::module_name_repetitions)]
pub trait DataSection {
    /// The start of the section to replace in the resource.
    fn start(&self) -> usize;
    /// The end of the section to replace in the resource.
    fn end(&self) -> usize;
    /// Returns a reference to the entirety of the data.
    fn data(&self) -> &[u8];
    /// Applies `self` to `resource`.
    ///
    /// Returns [`SliceBuf::filled`] if successful.
    ///
    /// # Errors
    ///
    /// Returns [`ApplyError::BufTooSmall`] if [`DataSection::data`] cannot fit in `resource`.
    #[allow(clippy::inline_always)]
    #[inline(always)]
    fn apply<T: AsMut<[u8]> + AsRef<[u8]>>(
        &self,
        resource: &mut SliceBuf<T>,
    ) -> Result<usize, ApplyError> {
        let new_size = add_iusize(resource.filled().len(), self.len_difference()).unwrap_or(0);
        if new_size > resource.capacity() || self.end() > resource.capacity() {
            return Err(ApplyError::BufTooSmall);
        }
        // Move all after self.end to self.start + self.new_len.
        // Copy self.contents to resource[self.start] with self.new_len

        // Copy the old data that's in the way of the new.
        // SAFETY: We guarantee above that we can move the bytes forward (or backward, this isn't a
        // problem) to self.start + self.new_len + (resource.filled - self.end) = self.new_len -
        // self.len_difference + resource.filled
        unsafe {
            std::ptr::copy(
                &resource.slice()[self.end()],
                &mut resource.slice_mut()[self.start() + self.new_len()],
                resource.filled().len().saturating_sub(self.end()),
            );
        }

        // Copy data from `Section` to `resource`.
        // SAFETY: The write is guaranteed to have the space left, see `unsafe` block above.
        // They will never overlap, as the [`SliceBuf`] contains a mutable reference to the bytes,
        // they are exclusive.
        unsafe {
            std::ptr::copy_nonoverlapping(
                &self.data()[0],
                &mut resource.slice_mut()[self.start()],
                self.new_len(),
            );
        }
        resource.set_filled(new_size);
        Ok(resource.filled().len())
    }
}
impl<S: DataSection + ?Sized> Section for S {
    #[allow(clippy::inline_always)]
    #[inline(always)]
    fn start(&self) -> usize {
        self.start()
    }
    #[allow(clippy::inline_always)]
    #[inline(always)]
    fn end(&self) -> usize {
        self.end()
    }
    #[allow(clippy::inline_always)]
    #[inline(always)]
    fn new_len(&self) -> usize {
        self.data().len()
    }
}

// See the safety guarantees. We copy data. This does not contain any buffers.
/// A section without any data.
/// Used in the internal log to track what has changed.
#[allow(clippy::unsafe_derive_deserialize)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Empty {
    start: usize,
    end: usize,
    len: usize,
}
impl Empty {
    pub(crate) fn new<S: Section>(section: &S) -> Self {
        Self {
            start: section.start(),
            end: section.end(),
            len: section.new_len(),
        }
    }
    /// The reverse of [`DataSection::apply`].
    /// Puts the data gathered from undoing the `apply` in a [`VecSection`].
    ///
    /// # Errors
    ///
    /// Returns an error if the new data cannot fit in `resource`.
    #[allow(clippy::inline_always)]
    #[inline(always)]
    pub(crate) fn revert<T: AsMut<[u8]> + AsRef<[u8]>>(
        &self,
        resource: &mut SliceBuf<T>,
    ) -> Result<VecSection, ApplyError> {
        let new_size = add_iusize(resource.filled().len(), -self.len_difference())
            .map_err(|()| ApplyError::BufTooSmall)?;
        if new_size > resource.capacity()
            || self.start() + self.new_len() > resource.capacity()
            || self.end() > resource.capacity()
        {
            return Err(ApplyError::BufTooSmall);
        }

        let mut section = VecSection::new(self.start(), self.end(), vec![0; self.new_len()]);

        // Copy data from `resource` to `section`.
        // SAFETY: we check that resource[self.start() + self.new_len()] is valid.
        // Section is created to house the data.
        // We have copied data to section, which fills it.
        unsafe {
            std::ptr::copy_nonoverlapping(
                &resource.slice()[self.start()],
                &mut section.data[0],
                self.new_len(),
            );
            section.data.set_len(section.data().len());
        }
        // Copy the "old" data back in place of the new.
        // SAFETY: Here, we simply copy the rest of the resource to later in the resource.
        // If it's out of bounds, it'll copy 0 bytes, which is sound.
        unsafe {
            std::ptr::copy(
                &resource.slice()[self.start() + self.new_len()],
                &mut resource.slice_mut()[self.end()],
                resource.filled().len().saturating_sub(self.end()),
            );
        }

        // If we added bytes here, fill with zeroes, to simplify debugging and avoid random data.
        if self.len_difference() < 0 {
            // We've checked that with the if statement above.
            #[allow(clippy::cast_sign_loss)]
            let to_fill = (0 - self.len_difference()) as usize;
            unsafe {
                std::ptr::write_bytes(
                    &mut resource.slice_mut()[self.start() + self.new_len()],
                    0,
                    to_fill,
                );
            }
        }

        resource.set_filled(new_size);
        Ok(section)
    }
}
impl Section for Empty {
    #[allow(clippy::inline_always)]
    #[inline(always)]
    fn start(&self) -> usize {
        self.start
    }
    #[allow(clippy::inline_always)]
    #[inline(always)]
    fn end(&self) -> usize {
        self.end
    }
    #[allow(clippy::inline_always)]
    #[inline(always)]
    fn new_len(&self) -> usize {
        self.len
    }
}

/// A selection of data in a resource.
///
/// Comparable to `slice` functions in various languages (e.g. [JS](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Array/splice)).
// We can deserialize this this data without fear of getting an incorrect buffer - serde `Vec::push`es elements.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
#[must_use]
#[allow(clippy::module_name_repetitions)]
pub struct VecSection {
    /// The start of the previous data in the resource.
    start: usize,
    /// The end of the previous data in the resource.
    pub(crate) end: usize,
    /// A reference to the data.
    data: Vec<u8>,
}
impl VecSection {
    /// Assembles a new section using `start` as the start of the original data, `end` as the
    /// terminator, and `data` to fill the space between `start` and `end`.
    ///
    /// If `data` is longer than `end-start`, the buffer (that this section will be applied to) is grown. If it's shorter, the buffer will
    /// be truncated.
    ///
    /// # Panics
    ///
    /// `start` must be less than or equal to `end`.
    pub fn new(start: usize, end: usize, data: Vec<u8>) -> Self {
        assert!(
            start <= end,
            "passed the start of data ({:?}) after the end of it({:?})",
            start,
            end
        );
        Self { start, end, data }
    }
    /// Creates a new section representing the entire resource.
    ///
    /// `base_resource_len` should be the length of the resource in the public storage.
    /// `data` is the new `data`, only known by us.
    ///
    /// `TODO`: Why is the `base_resource_len` the "old" data's length?
    ///
    /// Useful to use when passing data for [`crate::event::Modify::new`] to diff.
    pub fn whole_resource(base_resource_len: usize, data: Vec<u8>) -> Self {
        Self::new(0, base_resource_len, data)
    }
}
impl DataSection for VecSection {
    #[allow(clippy::inline_always)]
    #[inline(always)]
    fn start(&self) -> usize {
        self.start
    }
    #[allow(clippy::inline_always)]
    #[inline(always)]
    fn end(&self) -> usize {
        self.end
    }
    #[allow(clippy::inline_always)]
    #[inline(always)]
    fn data(&self) -> &[u8] {
        &self.data
    }
}
