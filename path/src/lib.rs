//! DISCLAIMER: This is modification of std::path module.
//!
//! Path manipulation.
//!
//! This module provides two types, [`PathBuf`] and [`Path`] (akin to [`String`]
//! and [`str`]), for working with paths abstractly. These types are thin
//! wrappers around [`String`] and [`str`] respectively, meaning that they work
//! directly on strings according to the local platform's path syntax.
//!
//! Paths can be parsed into [`Component`]s by iterating over the structure
//! returned by the [`components`] method on [`Path`]. [`Component`]s roughly
//! correspond to the substrings between path separators (`/` or `\`). You can
//! reconstruct an equivalent path from components with the [`push`] method on
//! [`PathBuf`]; note that the paths may differ syntactically by the
//! normalization described in the documentation for the [`components`] method.
//!
//! ## Simple usage
//!
//! Path manipulation includes both parsing components from slices and building
//! new owned paths.
//!
//! To parse a path, you can create a [`Path`] slice from a [`str`]
//! slice and start asking questions:
//!
//! ```
//! use std::path::Path;
//! use std::ffi::str;
//!
//! let path = Path::new("/tmp/foo/bar.txt");
//!
//! let parent = path.parent();
//! assert_eq!(parent, Some(Path::new("/tmp/foo")));
//!
//! let file_stem = path.file_stem();
//! assert_eq!(file_stem, Some(str::new("bar")));
//!
//! let extension = path.extension();
//! assert_eq!(extension, Some(str::new("txt")));
//! ```
//!
//! To build or modify paths, use [`PathBuf`]:
//!
//! ```
//! use std::path::PathBuf;
//!
//! // This way works...
//! let mut path = PathBuf::from("/");
//!
//! path.push("a");
//! path.push("b");
//!
//! path.set_extension(".rs");
//!
//! // ... but push is best used if you don't know everything up
//! // front. If you do, this way is better:
//! let path: PathBuf = ["/", "a", ""].iter().collect();
//! ```
//!
//! [`Component`]: ../../std/path/enum.Component.html
//! [`components`]: ../../std/path/struct.Path.html#method.components
//! [`PathBuf`]: ../../std/path/struct.PathBuf.html
//! [`Path`]: ../../std/path/struct.Path.html
//! [`push`]: ../../std/path/struct.PathBuf.html#method.push
//! [`String`]: ../../std/string/struct.String.html
//!
//! [`str`]: ../../std/primitive.str.html
//! [`String`]: ../../std/ffi/struct.String.html
//! [`str`]: ../../std/ffi/struct.str.html

#![no_std]
#![feature(extend_one, toowned_clone_into)]

extern crate alloc;

use alloc::borrow::{Borrow, Cow, ToOwned};
use alloc::boxed::Box;
use alloc::rc::Rc;
use alloc::str::FromStr;
use alloc::string::{String, ToString};
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::cmp;
use core::fmt;
use core::hash::{Hash, Hasher};
use core::iter::{self, FusedIterator};
use core::ops::{self, Deref};

#[inline]
fn is_sep_byte(b: u8) -> bool {
    b == b'/'
}

const MAIN_SEP: char = '/';

////////////////////////////////////////////////////////////////////////////////
// GENERAL NOTES
////////////////////////////////////////////////////////////////////////////////
//
// Parsing in this module is done by directly transmuting str to [u8] slices,
// taking advantage of the fact that str always encodes ASCII characters
// as-is.  Eventually, this transmutation should be replaced by direct uses of
// str APIs for parsing, but it will take a while for those to become
// available.

////////////////////////////////////////////////////////////////////////////////
// Exposed parsing helpers
////////////////////////////////////////////////////////////////////////////////

/// Determines whether the character is one of the permitted path
/// separators for the current platform.
///
/// # Examples
///
/// ```
/// use std::path;
///
/// assert!(path::is_separator('/')); // '/' works for both Unix and Windows
/// assert!(!path::is_separator('❤'));
/// ```
pub fn is_separator(c: char) -> bool {
    c.is_ascii() && is_sep_byte(c as u8)
}

////////////////////////////////////////////////////////////////////////////////
// Misc helpers
////////////////////////////////////////////////////////////////////////////////

// Iterate through `iter` while it matches `prefix`; return `None` if `prefix`
// is not a prefix of `iter`, otherwise return `Some(iter_after_prefix)` giving
// `iter` after having exhausted `prefix`.
fn iter_after<'a, 'b, I, J>(mut iter: I, mut prefix: J) -> Option<I>
where
    I: Iterator<Item = Component<'a>> + Clone,
    J: Iterator<Item = Component<'b>>,
{
    loop {
        let mut iter_next = iter.clone();
        match (iter_next.next(), prefix.next()) {
            (Some(ref x), Some(ref y)) if x == y => (),
            (Some(_), Some(_)) => return None,
            (Some(_), None) => return Some(iter),
            (None, None) => return Some(iter),
            (None, Some(_)) => return None,
        }
        iter = iter_next;
    }
}

// See note at the top of this module to understand why these are used:
//
// These casts are safe as str is internally a wrapper around [u8] on all
// platforms.
//
// Note that currently this relies on the special knowledge that libstd has;
// these types are single-element structs but are not marked repr(transparent)
// or repr(C) which would make these casts allowable outside std.
fn os_str_as_u8_slice(s: &str) -> &[u8] {
    unsafe { &*(s as *const str as *const [u8]) }
}

unsafe fn u8_slice_as_os_str(s: &[u8]) -> &str {
    &*(s as *const [u8] as *const str)
}

////////////////////////////////////////////////////////////////////////////////
// Cross-platform, iterator-independent parsing
////////////////////////////////////////////////////////////////////////////////

/// Says whether the first byte after the prefix is a separator.
fn has_physical_root(path: &[u8]) -> bool {
    !path.is_empty() && is_sep_byte(path[0])
}

// basic workhorse for splitting stem and extension
fn split_file_at_dot(file: &str) -> (Option<&str>, Option<&str>) {
    unsafe {
        if os_str_as_u8_slice(file) == b".." {
            return (Some(file), None);
        }

        // The unsafety here stems from converting between &str and &[u8]
        // and back. This is safe to do because (1) we only look at ASCII
        // contents of the encoding and (2) new &str values are produced
        // only from ASCII-bounded slices of existing &str values.

        let mut iter = os_str_as_u8_slice(file).rsplitn(2, |b| *b == b'.');
        let after = iter.next();
        let before = iter.next();
        if before == Some(b"") {
            (Some(file), None)
        } else {
            (
                before.map(|s| u8_slice_as_os_str(s)),
                after.map(|s| u8_slice_as_os_str(s)),
            )
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// The core iterators
////////////////////////////////////////////////////////////////////////////////

/// Component parsing works by a double-ended state machine; the cursors at the
/// front and back of the path each keep track of what parts of the path have
/// been consumed so far.
///
/// Going front to back, a path is made up of a prefix, a starting
/// directory component, and a body (of normal components)
#[derive(Copy, Clone, PartialEq, PartialOrd, Debug)]
enum State {
    Prefix = 0,   // c:
    StartDir = 1, // / or . or nothing
    Body = 2,     // foo/bar/baz
    Done = 3,
}

/// A single component of a path.
///
/// A `Component` roughly corresponds to a substring between path separators
/// (`/` or `\`).
///
/// This `enum` is created by iterating over [`Components`], which in turn is
/// created by the [`components`][`Path::components`] method on [`Path`].
///
/// # Examples
///
/// ```rust
/// use std::path::{Component, Path};
///
/// let path = Path::new("/tmp/foo/bar.txt");
/// let components = path.components().collect::<Vec<_>>();
/// assert_eq!(&components, &[
///     Component::RootDir,
///     Component::Normal("tmp".as_ref()),
///     Component::Normal("foo".as_ref()),
///     Component::Normal("bar.txt".as_ref()),
/// ]);
/// ```
///
/// [`Components`]: struct.Components.html
/// [`Path`]: struct.Path.html
/// [`Path::components`]: struct.Path.html#method.components
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]

pub enum Component<'a> {
    /// The root directory component, appears after any prefix and before
    /// anything else.
    ///
    /// It represents a separator that designates that a path starts from root.
    RootDir,

    /// A reference to the current directory, i.e., `.`.
    CurDir,

    /// A reference to the parent directory, i.e., `..`.
    ParentDir,

    /// A normal component, e.g., `a` and `b` in `a/b`.
    ///
    /// This variant is the most common one, it represents references to files
    /// or directories.
    Normal(&'a str),
}

impl<'a> Component<'a> {
    /// Extracts the underlying [`str`] slice.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::path::Path;
    ///
    /// let path = Path::new("./tmp/foo/bar.txt");
    /// let components: Vec<_> = path.components().map(|comp| comp.as_os_str()).collect();
    /// assert_eq!(&components, &[".", "tmp", "foo", "bar.txt"]);
    /// ```
    ///
    /// [`str`]: ../../std/ffi/struct.str.html

    pub fn as_os_str(self) -> &'a str {
        match self {
            Component::RootDir => "/",
            Component::CurDir => ".",
            Component::ParentDir => "..",
            Component::Normal(path) => path,
        }
    }
}

impl AsRef<str> for Component<'_> {
    fn as_ref(&self) -> &str {
        self.as_os_str()
    }
}

impl AsRef<Path> for Component<'_> {
    fn as_ref(&self) -> &Path {
        self.as_os_str().as_ref()
    }
}

/// An iterator over the [`Component`]s of a [`Path`].
///
/// This `struct` is created by the [`components`] method on [`Path`].
/// See its documentation for more.
///
/// # Examples
///
/// ```
/// use std::path::Path;
///
/// let path = Path::new("/tmp/foo/bar.txt");
///
/// for component in path.components() {
///     println!("{:?}", component);
/// }
/// ```
///
/// [`Component`]: enum.Component.html
/// [`components`]: struct.Path.html#method.components
/// [`Path`]: struct.Path.html
#[derive(Clone)]

pub struct Components<'a> {
    // The path left to parse components from
    path: &'a [u8],

    // true if path *physically* has a root separator; for most Windows
    // prefixes, it may have a "logical" rootseparator for the purposes of
    // normalization, e.g.,  \\server\share == \\server\share\.
    has_physical_root: bool,

    // The iterator is double-ended, and these two states keep track of what has
    // been produced from either end
    front: State,
    back: State,
}

/// An iterator over the [`Component`]s of a [`Path`], as [`str`] slices.
///
/// This `struct` is created by the [`iter`] method on [`Path`].
/// See its documentation for more.
///
/// [`Component`]: enum.Component.html
/// [`iter`]: struct.Path.html#method.iter
/// [`str`]: ../../std/ffi/struct.str.html
/// [`Path`]: struct.Path.html
#[derive(Clone)]

pub struct Iter<'a> {
    inner: Components<'a>,
}

impl fmt::Debug for Components<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        struct DebugHelper<'a>(&'a Path);

        impl fmt::Debug for DebugHelper<'_> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.debug_list().entries(self.0.components()).finish()
            }
        }

        f.debug_tuple("Components")
            .field(&DebugHelper(self.as_path()))
            .finish()
    }
}

impl<'a> Components<'a> {
    // Given the iteration so far, how much of the pre-State::Body path is left?
    #[inline]
    fn len_before_body(&self) -> usize {
        let root = if self.front <= State::StartDir && self.has_physical_root {
            1
        } else {
            0
        };
        let cur_dir = if self.front <= State::StartDir && self.include_cur_dir() {
            1
        } else {
            0
        };
        root + cur_dir
    }

    // is the iteration complete?
    #[inline]
    fn finished(&self) -> bool {
        self.front == State::Done || self.back == State::Done || self.front > self.back
    }

    #[inline]
    fn is_sep_byte(&self, b: u8) -> bool {
        is_sep_byte(b)
    }

    /// Extracts a slice corresponding to the portion of the path remaining for
    /// iteration.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::path::Path;
    ///
    /// let mut components = Path::new("/tmp/foo/bar.txt").components();
    /// components.next();
    /// components.next();
    ///
    /// assert_eq!(Path::new("foo/bar.txt"), components.as_path());
    /// ```
    pub fn as_path(&self) -> &'a Path {
        let mut comps = self.clone();
        if comps.front == State::Body {
            comps.trim_left();
        }
        if comps.back == State::Body {
            comps.trim_right();
        }
        unsafe { Path::from_u8_slice(comps.path) }
    }

    /// Is the *original* path rooted?
    fn has_root(&self) -> bool {
        self.has_physical_root
    }

    /// Should the normalized path include a leading . ?
    fn include_cur_dir(&self) -> bool {
        if self.has_root() {
            return false;
        }
        let mut iter = self.path.iter();
        match (iter.next(), iter.next()) {
            (Some(&b'.'), None) => true,
            (Some(&b'.'), Some(&b)) => self.is_sep_byte(b),
            _ => false,
        }
    }

    // parse a given byte sequence into the corresponding path component
    fn parse_single_component<'b>(&self, comp: &'b [u8]) -> Option<Component<'b>> {
        match comp {
            b"." => None, // . components are normalized away, except at
            // the beginning of a path, which is treated
            // separately via `include_cur_dir`
            b".." => Some(Component::ParentDir),
            b"" => None,
            _ => Some(Component::Normal(unsafe { u8_slice_as_os_str(comp) })),
        }
    }

    // parse a component from the left, saying how many bytes to consume to
    // remove the component
    fn parse_next_component(&self) -> (usize, Option<Component<'a>>) {
        debug_assert!(self.front == State::Body);
        let (extra, comp) = match self.path.iter().position(|b| self.is_sep_byte(*b)) {
            None => (0, self.path),
            Some(i) => (1, &self.path[..i]),
        };
        (comp.len() + extra, self.parse_single_component(comp))
    }

    // parse a component from the right, saying how many bytes to consume to
    // remove the component
    fn parse_next_component_back(&self) -> (usize, Option<Component<'a>>) {
        debug_assert!(self.back == State::Body);
        let start = self.len_before_body();
        let (extra, comp) = match self.path[start..]
            .iter()
            .rposition(|b| self.is_sep_byte(*b))
        {
            None => (0, &self.path[start..]),
            Some(i) => (1, &self.path[start + i + 1..]),
        };
        (comp.len() + extra, self.parse_single_component(comp))
    }

    // trim away repeated separators (i.e., empty components) on the left
    fn trim_left(&mut self) {
        while !self.path.is_empty() {
            let (size, comp) = self.parse_next_component();
            if comp.is_some() {
                return;
            } else {
                self.path = &self.path[size..];
            }
        }
    }

    // trim away repeated separators (i.e., empty components) on the right
    fn trim_right(&mut self) {
        while self.path.len() > self.len_before_body() {
            let (size, comp) = self.parse_next_component_back();
            if comp.is_some() {
                return;
            } else {
                self.path = &self.path[..self.path.len() - size];
            }
        }
    }
}

impl AsRef<Path> for Components<'_> {
    fn as_ref(&self) -> &Path {
        self.as_path()
    }
}

impl AsRef<str> for Components<'_> {
    fn as_ref(&self) -> &str {
        &self.as_path().inner
    }
}

impl fmt::Debug for Iter<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        struct DebugHelper<'a>(&'a Path);

        impl fmt::Debug for DebugHelper<'_> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.debug_list().entries(self.0.iter()).finish()
            }
        }

        f.debug_tuple("Iter")
            .field(&DebugHelper(self.as_path()))
            .finish()
    }
}

impl<'a> Iter<'a> {
    /// Extracts a slice corresponding to the portion of the path remaining for
    /// iteration.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::path::Path;
    ///
    /// let mut iter = Path::new("/tmp/foo/bar.txt").iter();
    /// iter.next();
    /// iter.next();
    ///
    /// assert_eq!(Path::new("foo/bar.txt"), iter.as_path());
    /// ```

    pub fn as_path(&self) -> &'a Path {
        self.inner.as_path()
    }
}

impl AsRef<Path> for Iter<'_> {
    fn as_ref(&self) -> &Path {
        self.as_path()
    }
}

impl AsRef<str> for Iter<'_> {
    fn as_ref(&self) -> &str {
        &self.as_path().inner
    }
}

impl<'a> Iterator for Iter<'a> {
    type Item = &'a str;

    fn next(&mut self) -> Option<&'a str> {
        self.inner.next().map(Component::as_os_str)
    }
}

impl<'a> DoubleEndedIterator for Iter<'a> {
    fn next_back(&mut self) -> Option<&'a str> {
        self.inner.next_back().map(Component::as_os_str)
    }
}

impl FusedIterator for Iter<'_> {}

impl<'a> Iterator for Components<'a> {
    type Item = Component<'a>;

    fn next(&mut self) -> Option<Component<'a>> {
        while !self.finished() {
            match self.front {
                State::Prefix => {
                    self.front = State::StartDir;
                }
                State::StartDir => {
                    self.front = State::Body;
                    if self.has_physical_root {
                        debug_assert!(!self.path.is_empty());
                        self.path = &self.path[1..];
                        return Some(Component::RootDir);
                    } else if self.include_cur_dir() {
                        debug_assert!(!self.path.is_empty());
                        self.path = &self.path[1..];
                        return Some(Component::CurDir);
                    }
                }
                State::Body if !self.path.is_empty() => {
                    let (size, comp) = self.parse_next_component();
                    self.path = &self.path[size..];
                    if comp.is_some() {
                        return comp;
                    }
                }
                State::Body => {
                    self.front = State::Done;
                }
                State::Done => unreachable!(),
            }
        }
        None
    }
}

impl<'a> DoubleEndedIterator for Components<'a> {
    fn next_back(&mut self) -> Option<Component<'a>> {
        while !self.finished() {
            match self.back {
                State::Body if self.path.len() > self.len_before_body() => {
                    let (size, comp) = self.parse_next_component_back();
                    self.path = &self.path[..self.path.len() - size];
                    if comp.is_some() {
                        return comp;
                    }
                }
                State::Body => {
                    self.back = State::StartDir;
                }
                State::StartDir => {
                    self.back = State::Prefix;
                    if self.has_physical_root {
                        self.path = &self.path[..self.path.len() - 1];
                        return Some(Component::RootDir);
                    } else if self.include_cur_dir() {
                        self.path = &self.path[..self.path.len() - 1];
                        return Some(Component::CurDir);
                    }
                }
                State::Prefix => {
                    self.back = State::Done;
                    return None;
                }
                State::Done => unreachable!(),
            }
        }
        None
    }
}

impl FusedIterator for Components<'_> {}

impl<'a> cmp::PartialEq for Components<'a> {
    fn eq(&self, other: &Components<'a>) -> bool {
        Iterator::eq(self.clone(), other.clone())
    }
}

impl cmp::Eq for Components<'_> {}

impl<'a> cmp::PartialOrd for Components<'a> {
    fn partial_cmp(&self, other: &Components<'a>) -> Option<cmp::Ordering> {
        Iterator::partial_cmp(self.clone(), other.clone())
    }
}

impl cmp::Ord for Components<'_> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        Iterator::cmp(self.clone(), other.clone())
    }
}

/// An iterator over [`Path`] and its ancestors.
///
/// This `struct` is created by the [`ancestors`] method on [`Path`].
/// See its documentation for more.
///
/// # Examples
///
/// ```
/// use std::path::Path;
///
/// let path = Path::new("/foo/bar");
///
/// for ancestor in path.ancestors() {
///     println!("{}", ancestor.display());
/// }
/// ```
///
/// [`ancestors`]: struct.Path.html#method.ancestors
/// [`Path`]: struct.Path.html
#[derive(Copy, Clone, Debug)]

pub struct Ancestors<'a> {
    next: Option<&'a Path>,
}

impl<'a> Iterator for Ancestors<'a> {
    type Item = &'a Path;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.next;
        self.next = next.and_then(Path::parent);
        next
    }
}

impl FusedIterator for Ancestors<'_> {}

////////////////////////////////////////////////////////////////////////////////
// Basic types and traits
////////////////////////////////////////////////////////////////////////////////

/// An owned, mutable path (akin to [`String`]).
///
/// This type provides methods like [`push`] and [`set_extension`] that mutate
/// the path in place. It also implements [`Deref`] to [`Path`], meaning that
/// all methods on [`Path`] slices are available on `PathBuf` values as well.
///
/// [`String`]: ../string/struct.String.html
/// [`Path`]: struct.Path.html
/// [`push`]: struct.PathBuf.html#method.push
/// [`set_extension`]: struct.PathBuf.html#method.set_extension
/// [`Deref`]: ../ops/trait.Deref.html
///
/// More details about the overall approach can be found in
/// the [module documentation](index.html).
///
/// # Examples
///
/// You can use [`push`] to build up a `PathBuf` from
/// components:
///
/// ```
/// use std::path::PathBuf;
///
/// let mut path = PathBuf::new();
///
/// path.push(r"C:\");
/// path.push("windows");
/// path.push("system32");
///
/// path.set_extension("dll");
/// ```
///
/// However, [`push`] is best used for dynamic situations. This is a better way
/// to do this when you know all of the components ahead of time:
///
/// ```
/// use std::path::PathBuf;
///
/// let path: PathBuf = [r"C:\", "windows", "system32.dll"].iter().collect();
/// ```
///
/// We can still do better than this! Since these are all strings, we can use
/// `From::from`:
///
/// ```
/// use std::path::PathBuf;
///
/// let path = PathBuf::from(r"C:\windows\system32.dll");
/// ```
///
/// Which method works best depends on what kind of situation you're in.
#[derive(Clone)]

// FIXME:
// `PathBuf::as_mut_vec` current implementation relies
// on `PathBuf` being layout-compatible with `Vec<u8>`.
// When attribute privacy is implemented, `PathBuf` should be annotated as
// `#[repr(transparent)]`. Anyway, `PathBuf` representation and layout are
// considered implementation detail, are not documented and must not be relied
// upon.
pub struct PathBuf {
    inner: String,
}

impl PathBuf {
    fn as_mut_vec(&mut self) -> &mut Vec<u8> {
        unsafe { &mut *(self as *mut PathBuf as *mut Vec<u8>) }
    }

    /// Allocates an empty `PathBuf`.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::path::PathBuf;
    ///
    /// let path = PathBuf::new();
    /// ```

    pub fn new() -> PathBuf {
        PathBuf {
            inner: String::new(),
        }
    }

    /// Creates a new `PathBuf` with a given capacity used to create the
    /// internal [`String`]. See [`with_capacity`] defined on [`String`].
    ///
    /// # Examples
    ///
    /// ```
    /// use std::path::PathBuf;
    ///
    /// let mut path = PathBuf::with_capacity(10);
    /// let capacity = path.capacity();
    ///
    /// // This push is done without reallocating
    /// path.push(r"C:\");
    ///
    /// assert_eq!(capacity, path.capacity());
    /// ```
    ///
    /// [`with_capacity`]: ../ffi/struct.String.html#method.with_capacity
    /// [`String`]: ../ffi/struct.String.html

    pub fn with_capacity(capacity: usize) -> PathBuf {
        PathBuf {
            inner: String::with_capacity(capacity),
        }
    }

    /// Coerces to a [`Path`] slice.
    ///
    /// [`Path`]: struct.Path.html
    ///
    /// # Examples
    ///
    /// ```
    /// use std::path::{Path, PathBuf};
    ///
    /// let p = PathBuf::from("/test");
    /// assert_eq!(Path::new("/test"), p.as_path());
    /// ```

    pub fn as_path(&self) -> &Path {
        self
    }

    /// Extends `self` with `path`.
    ///
    /// If `path` is absolute, it replaces the current path.
    ///
    /// On Windows:
    ///
    /// * if `path` has a root but no prefix (e.g., `\windows`), it replaces
    ///   everything except for the prefix (if any) of `self`.
    /// * if `path` has a prefix but no root, it replaces `self`.
    ///
    /// # Examples
    ///
    /// Pushing a relative path extends the existing path:
    ///
    /// ```
    /// use std::path::PathBuf;
    ///
    /// let mut path = PathBuf::from("/tmp");
    /// path.push("file.bk");
    /// assert_eq!(path, PathBuf::from("/tmp/file.bk"));
    /// ```
    ///
    /// Pushing an absolute path replaces the existing path:
    ///
    /// ```
    /// use std::path::PathBuf;
    ///
    /// let mut path = PathBuf::from("/tmp");
    /// path.push("/etc");
    /// assert_eq!(path, PathBuf::from("/etc"));
    /// ```

    pub fn push<P: AsRef<Path>>(&mut self, path: P) {
        self._push(path.as_ref())
    }

    fn _push(&mut self, path: &Path) {
        // in general, a separator is needed if the rightmost byte is not a separator
        let need_sep = self
            .as_mut_vec()
            .last()
            .map(|c| !is_sep_byte(*c))
            .unwrap_or(false);

        // absolute `path` replaces `self`
        if path.has_root() {
            self.as_mut_vec().truncate(0);
        // `path` is a pure relative path
        } else if need_sep {
            self.inner.push(MAIN_SEP);
        }

        self.inner.push_str(path.to_str().unwrap());
    }

    /// Truncates `self` to [`self.parent`].
    ///
    /// Returns `false` and does nothing if [`self.parent`] is [`None`].
    /// Otherwise, returns `true`.
    ///
    /// [`None`]: ../../std/option/enum.Option.html#variant.None
    /// [`self.parent`]: struct.PathBuf.html#method.parent
    ///
    /// # Examples
    ///
    /// ```
    /// use std::path::{Path, PathBuf};
    ///
    /// let mut p = PathBuf::from("/test/test.rs");
    ///
    /// p.pop();
    /// assert_eq!(Path::new("/test"), p);
    /// p.pop();
    /// assert_eq!(Path::new("/"), p);
    /// ```
    pub fn pop(&mut self) -> bool {
        match self.parent().map(|p| p.as_u8_slice().len()) {
            Some(len) => {
                self.as_mut_vec().truncate(len);
                true
            }
            None => false,
        }
    }

    /// Updates [`self.file_name`] to `file_name`.
    ///
    /// If [`self.file_name`] was [`None`], this is equivalent to pushing
    /// `file_name`.
    ///
    /// Otherwise it is equivalent to calling [`pop`] and then pushing
    /// `file_name`. The new path will be a sibling of the original path.
    /// (That is, it will have the same parent.)
    ///
    /// [`self.file_name`]: struct.PathBuf.html#method.file_name
    /// [`None`]: ../../std/option/enum.Option.html#variant.None
    /// [`pop`]: struct.PathBuf.html#method.pop
    ///
    /// # Examples
    ///
    /// ```
    /// use std::path::PathBuf;
    ///
    /// let mut buf = PathBuf::from("/");
    /// assert!(buf.file_name() == None);
    /// buf.set_file_name("bar");
    /// assert!(buf == PathBuf::from("/bar"));
    /// assert!(buf.file_name().is_some());
    /// buf.set_file_name("baz.txt");
    /// assert!(buf == PathBuf::from("/baz.txt"));
    /// ```

    pub fn set_file_name<S: AsRef<str>>(&mut self, file_name: S) {
        self._set_file_name(file_name.as_ref())
    }

    fn _set_file_name(&mut self, file_name: &str) {
        if self.file_name().is_some() {
            let popped = self.pop();
            debug_assert!(popped);
        }
        self.push(file_name);
    }

    /// Updates [`self.extension`] to `extension`.
    ///
    /// Returns `false` and does nothing if [`self.file_name`] is [`None`],
    /// returns `true` and updates the extension otherwise.
    ///
    /// If [`self.extension`] is [`None`], the extension is added; otherwise
    /// it is replaced.
    ///
    /// [`self.file_name`]: struct.PathBuf.html#method.file_name
    /// [`self.extension`]: struct.PathBuf.html#method.extension
    /// [`None`]: ../../std/option/enum.Option.html#variant.None
    ///
    /// # Examples
    ///
    /// ```
    /// use std::path::{Path, PathBuf};
    ///
    /// let mut p = PathBuf::from("/feel/the");
    ///
    /// p.set_extension("force");
    /// assert_eq!(Path::new("/feel/the.force"), p.as_path());
    ///
    /// p.set_extension("dark_side");
    /// assert_eq!(Path::new("/feel/the.dark_side"), p.as_path());
    /// ```
    pub fn set_extension<S: AsRef<str>>(&mut self, extension: S) -> bool {
        self._set_extension(extension.as_ref())
    }

    fn _set_extension(&mut self, extension: &str) -> bool {
        let file_stem = match self.file_stem() {
            None => return false,
            Some(f) => os_str_as_u8_slice(f),
        };

        // truncate until right after the file stem
        let end_file_stem = file_stem[file_stem.len()..].as_ptr() as usize;
        let start = os_str_as_u8_slice(&self.inner).as_ptr() as usize;
        let v = self.as_mut_vec();
        v.truncate(end_file_stem.wrapping_sub(start));

        // add the new extension, if any
        let new = os_str_as_u8_slice(extension);
        if !new.is_empty() {
            v.reserve_exact(new.len() + 1);
            v.push(b'.');
            v.extend_from_slice(new);
        }

        true
    }

    /// Consumes the `PathBuf`, yielding its internal [`String`] storage.
    ///
    /// [`String`]: ../ffi/struct.String.html
    ///
    /// # Examples
    ///
    /// ```
    /// use std::path::PathBuf;
    ///
    /// let p = PathBuf::from("/the/head");
    /// let os_str = p.into_os_string();
    /// ```
    pub fn into_os_string(self) -> String {
        self.inner
    }

    /// Converts this `PathBuf` into a [boxed][`Box`] [`Path`].
    ///
    /// [`Box`]: ../../std/boxed/struct.Box.html
    /// [`Path`]: struct.Path.html
    pub fn into_boxed_path(self) -> Box<Path> {
        let rw = Box::into_raw(self.inner.into_boxed_str()) as *mut Path;
        unsafe { Box::from_raw(rw) }
    }

    /// Invokes [`capacity`] on the underlying instance of [`String`].
    ///
    /// [`capacity`]: ../ffi/struct.String.html#method.capacity
    /// [`String`]: ../ffi/struct.String.html

    pub fn capacity(&self) -> usize {
        self.inner.capacity()
    }

    /// Invokes [`clear`] on the underlying instance of [`String`].
    ///
    /// [`clear`]: ../ffi/struct.String.html#method.clear
    /// [`String`]: ../ffi/struct.String.html
    pub fn clear(&mut self) {
        self.inner.clear()
    }

    /// Invokes [`reserve`] on the underlying instance of [`String`].
    ///
    /// [`reserve`]: ../ffi/struct.String.html#method.reserve
    /// [`String`]: ../ffi/struct.String.html
    pub fn reserve(&mut self, additional: usize) {
        self.inner.reserve(additional)
    }

    /// Invokes [`reserve_exact`] on the underlying instance of [`String`].
    ///
    /// [`reserve_exact`]: ../ffi/struct.String.html#method.reserve_exact
    /// [`String`]: ../ffi/struct.String.html
    pub fn reserve_exact(&mut self, additional: usize) {
        self.inner.reserve_exact(additional)
    }

    /// Invokes [`shrink_to_fit`] on the underlying instance of [`String`].
    ///
    /// [`shrink_to_fit`]: ../ffi/struct.String.html#method.shrink_to_fit
    /// [`String`]: ../ffi/struct.String.html
    pub fn shrink_to_fit(&mut self) {
        self.inner.shrink_to_fit()
    }
}

impl From<&Path> for Box<Path> {
    fn from(path: &Path) -> Box<Path> {
        let boxed: Box<str> = path.inner.into();
        let rw = Box::into_raw(boxed) as *mut Path;
        unsafe { Box::from_raw(rw) }
    }
}

impl From<Cow<'_, Path>> for Box<Path> {
    #[inline]
    fn from(cow: Cow<'_, Path>) -> Box<Path> {
        match cow {
            Cow::Borrowed(path) => Box::from(path),
            Cow::Owned(path) => Box::from(path),
        }
    }
}

impl From<Box<Path>> for PathBuf {
    /// Converts a `Box<Path>` into a `PathBuf`
    ///
    /// This conversion does not allocate or copy memory.
    fn from(boxed: Box<Path>) -> PathBuf {
        boxed.into_path_buf()
    }
}

impl From<PathBuf> for Box<Path> {
    /// Converts a `PathBuf` into a `Box<Path>`
    ///
    /// This conversion currently should not allocate memory,
    /// but this behavior is not guaranteed on all platforms or in all future
    /// versions.
    fn from(p: PathBuf) -> Box<Path> {
        p.into_boxed_path()
    }
}

impl Clone for Box<Path> {
    #[inline]
    fn clone(&self) -> Self {
        self.to_path_buf().into_boxed_path()
    }
}

impl<T: ?Sized + AsRef<str>> From<&T> for PathBuf {
    fn from(s: &T) -> PathBuf {
        PathBuf::from(s.as_ref().to_string())
    }
}

impl From<String> for PathBuf {
    /// Converts a `String` into a `PathBuf`
    ///
    /// This conversion does not allocate or copy memory.
    #[inline]
    fn from(s: String) -> PathBuf {
        PathBuf { inner: s }
    }
}

impl From<PathBuf> for String {
    /// Converts a `PathBuf` into a `String`
    ///
    /// This conversion does not allocate or copy memory.
    fn from(path_buf: PathBuf) -> String {
        path_buf.inner
    }
}

impl FromStr for PathBuf {
    type Err = core::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(PathBuf::from(s))
    }
}

impl<P: AsRef<Path>> iter::FromIterator<P> for PathBuf {
    fn from_iter<I: IntoIterator<Item = P>>(iter: I) -> PathBuf {
        let mut buf = PathBuf::new();
        buf.extend(iter);
        buf
    }
}

impl<P: AsRef<Path>> iter::Extend<P> for PathBuf {
    fn extend<I: IntoIterator<Item = P>>(&mut self, iter: I) {
        iter.into_iter().for_each(move |p| self.push(p.as_ref()));
    }

    #[inline]
    fn extend_one(&mut self, p: P) {
        self.push(p.as_ref());
    }
}

impl fmt::Debug for PathBuf {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, formatter)
    }
}

impl ops::Deref for PathBuf {
    type Target = Path;
    #[inline]
    fn deref(&self) -> &Path {
        Path::new(&self.inner)
    }
}

impl Borrow<Path> for PathBuf {
    fn borrow(&self) -> &Path {
        self.deref()
    }
}

impl Default for PathBuf {
    fn default() -> Self {
        PathBuf::new()
    }
}

impl<'a> From<&'a Path> for Cow<'a, Path> {
    #[inline]
    fn from(s: &'a Path) -> Cow<'a, Path> {
        Cow::Borrowed(s)
    }
}

impl<'a> From<PathBuf> for Cow<'a, Path> {
    #[inline]
    fn from(s: PathBuf) -> Cow<'a, Path> {
        Cow::Owned(s)
    }
}

impl<'a> From<&'a PathBuf> for Cow<'a, Path> {
    #[inline]
    fn from(p: &'a PathBuf) -> Cow<'a, Path> {
        Cow::Borrowed(p.as_path())
    }
}

impl<'a> From<Cow<'a, Path>> for PathBuf {
    #[inline]
    fn from(p: Cow<'a, Path>) -> Self {
        p.into_owned()
    }
}

impl From<PathBuf> for Arc<Path> {
    /// Converts a `PathBuf` into an `Arc` by moving the `PathBuf` data into a
    /// new `Arc` buffer.
    #[inline]
    fn from(s: PathBuf) -> Arc<Path> {
        let arc: Arc<str> = Arc::from(s.into_os_string());
        unsafe { Arc::from_raw(Arc::into_raw(arc) as *const Path) }
    }
}

impl From<&Path> for Arc<Path> {
    /// Converts a `Path` into an `Arc` by copying the `Path` data into a new
    /// `Arc` buffer.
    #[inline]
    fn from(s: &Path) -> Arc<Path> {
        let arc: Arc<str> = Arc::from(&s.inner);
        unsafe { Arc::from_raw(Arc::into_raw(arc) as *const Path) }
    }
}

impl From<PathBuf> for Rc<Path> {
    /// Converts a `PathBuf` into an `Rc` by moving the `PathBuf` data into a
    /// new `Rc` buffer.
    #[inline]
    fn from(s: PathBuf) -> Rc<Path> {
        let rc: Rc<str> = Rc::from(s.inner.as_str());
        unsafe { Rc::from_raw(Rc::into_raw(rc) as *const Path) }
    }
}

impl From<&Path> for Rc<Path> {
    /// Converts a `Path` into an `Rc` by copying the `Path` data into a new
    /// `Rc` buffer.
    #[inline]
    fn from(s: &Path) -> Rc<Path> {
        let rc: Rc<str> = Rc::from(&s.inner);
        unsafe { Rc::from_raw(Rc::into_raw(rc) as *const Path) }
    }
}

impl ToOwned for Path {
    type Owned = PathBuf;
    fn to_owned(&self) -> PathBuf {
        self.to_path_buf()
    }
    fn clone_into(&self, target: &mut PathBuf) {
        self.inner.clone_into(&mut target.inner);
    }
}

impl cmp::PartialEq for PathBuf {
    fn eq(&self, other: &PathBuf) -> bool {
        self.components() == other.components()
    }
}

impl Hash for PathBuf {
    fn hash<H: Hasher>(&self, h: &mut H) {
        self.as_path().hash(h)
    }
}

impl cmp::Eq for PathBuf {}

impl cmp::PartialOrd for PathBuf {
    fn partial_cmp(&self, other: &PathBuf) -> Option<cmp::Ordering> {
        self.components().partial_cmp(other.components())
    }
}

impl cmp::Ord for PathBuf {
    fn cmp(&self, other: &PathBuf) -> cmp::Ordering {
        self.components().cmp(other.components())
    }
}

impl AsRef<str> for PathBuf {
    fn as_ref(&self) -> &str {
        &self.inner[..]
    }
}

/// A slice of a path (akin to [`str`]).
///
/// This type supports a number of operations for inspecting a path, including
/// breaking the path into its components (separated by `/` on Unix and by
/// either `/` or `\` on Windows), extracting the file name, determining whether
/// the path is absolute, and so on.
///
/// This is an *unsized* type, meaning that it must always be used behind a
/// pointer like `&` or [`Box`]. For an owned version of this type,
/// see [`PathBuf`].
///
/// [`str`]: ../primitive.str.html
/// [`Box`]: ../boxed/struct.Box.html
/// [`PathBuf`]: struct.PathBuf.html
///
/// More details about the overall approach can be found in
/// the [module documentation](index.html).
///
/// # Examples
///
/// ```
/// use std::path::Path;
/// use std::ffi::str;
///
/// // Note: this example does work on Windows
/// let path = Path::new("./foo/bar.txt");
///
/// let parent = path.parent();
/// assert_eq!(parent, Some(Path::new("./foo")));
///
/// let file_stem = path.file_stem();
/// assert_eq!(file_stem, Some(str::new("bar")));
///
/// let extension = path.extension();
/// assert_eq!(extension, Some(str::new("txt")));
/// ```

// FIXME:
// `Path::new` current implementation relies
// on `Path` being layout-compatible with `str`.
// When attribute privacy is implemented, `Path` should be annotated as
// `#[repr(transparent)]`. Anyway, `Path` representation and layout are
// considered implementation detail, are not documented and must not be relied
// upon.
pub struct Path {
    inner: str,
}

/// An error returned from [`Path::strip_prefix`][`strip_prefix`] if the prefix
/// was not found.
///
/// This `struct` is created by the [`strip_prefix`] method on [`Path`].
/// See its documentation for more.
///
/// [`strip_prefix`]: struct.Path.html#method.strip_prefix
/// [`Path`]: struct.Path.html
#[derive(Debug, Clone, PartialEq, Eq)]

pub struct StripPrefixError(());

impl Path {
    // The following (private!) function allows construction of a path from a u8
    // slice, which is only safe when it is known to follow the str encoding.
    unsafe fn from_u8_slice(s: &[u8]) -> &Path {
        Path::new(u8_slice_as_os_str(s))
    }
    // The following (private!) function reveals the byte encoding used for str.
    fn as_u8_slice(&self) -> &[u8] {
        self.inner.as_bytes()
    }

    /// Directly wraps a string slice as a `Path` slice.
    ///
    /// This is a cost-free conversion.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::path::Path;
    ///
    /// Path::new("foo.txt");
    /// ```
    ///
    /// You can create `Path`s from `String`s, or even other `Path`s:
    ///
    /// ```
    /// use std::path::Path;
    ///
    /// let string = String::from("foo.txt");
    /// let from_string = Path::new(&string);
    /// let from_path = Path::new(&from_string);
    /// assert_eq!(from_string, from_path);
    /// ```
    pub fn new<S: AsRef<str> + ?Sized>(s: &S) -> &Path {
        unsafe { &*(s.as_ref() as *const str as *const Path) }
    }

    /// Yields a [`&str`] slice if the `Path` is valid unicode.
    ///
    /// This conversion may entail doing a check for UTF-8 validity.
    /// Note that validation is performed because non-UTF-8 strings are
    /// perfectly valid for some OS.
    ///
    /// [`&str`]: ../primitive.str.html
    ///
    /// # Examples
    ///
    /// ```
    /// use std::path::Path;
    ///
    /// let path = Path::new("foo.txt");
    /// assert_eq!(path.to_str(), Some("foo.txt"));
    /// ```
    pub fn to_str(&self) -> Option<&str> {
        Some(&self.inner)
    }

    /// Converts a `Path` to an owned [`PathBuf`].
    ///
    /// [`PathBuf`]: struct.PathBuf.html
    ///
    /// # Examples
    ///
    /// ```
    /// use std::path::Path;
    ///
    /// let path_buf = Path::new("foo.txt").to_path_buf();
    /// assert_eq!(path_buf, std::path::PathBuf::from("foo.txt"));
    /// ```
    pub fn to_path_buf(&self) -> PathBuf {
        PathBuf::from(self.inner.to_string())
    }

    /// Returns `true` if the `Path` is absolute, i.e., if it is independent of
    /// the current directory.
    ///
    /// # Examples
    ///
    /// ```
    /// use kcore::fs::path::Path;
    ///
    /// assert!(!Path::new("foo.txt").is_absolute());
    /// ```
    ///
    /// [`has_root`]: #method.has_root
    pub fn is_absolute(&self) -> bool {
        self.has_root()
    }

    /// Returns `true` if the `Path` is relative, i.e., not absolute.
    ///
    /// See [`is_absolute`]'s documentation for more details.
    ///
    /// # Examples
    ///
    /// ```
    /// use kcore::fs::Path;
    ///
    /// assert!(Path::new("foo.txt").is_relative());
    /// ```
    ///
    /// [`is_absolute`]: #method.is_absolute
    pub fn is_relative(&self) -> bool {
        !self.is_absolute()
    }

    /// Returns `true` if the `Path` has a root.
    ///
    /// * On Unix, a path has a root if it begins with `/`.
    ///
    /// # Examples
    ///
    /// ```
    /// use kcore::fs::path::Path;
    ///
    /// assert!(Path::new("/etc/passwd").has_root());
    /// ```
    pub fn has_root(&self) -> bool {
        self.components().has_root()
    }

    /// Returns the `Path` without its final component, if there is one.
    ///
    /// Returns [`None`] if the path terminates in a root or prefix.
    ///
    /// [`None`]: ../../std/option/enum.Option.html#variant.None
    ///
    /// # Examples
    ///
    /// ```
    /// use std::path::Path;
    ///
    /// let path = Path::new("/foo/bar");
    /// let parent = path.parent().unwrap();
    /// assert_eq!(parent, Path::new("/foo"));
    ///
    /// let grand_parent = parent.parent().unwrap();
    /// assert_eq!(grand_parent, Path::new("/"));
    /// assert_eq!(grand_parent.parent(), None);
    /// ```
    pub fn parent(&self) -> Option<&Path> {
        let mut comps = self.components();
        let comp = comps.next_back();
        comp.and_then(|p| match p {
            Component::Normal(_) | Component::CurDir | Component::ParentDir => {
                Some(comps.as_path())
            }
            _ => None,
        })
    }

    /// Produces an iterator over `Path` and its ancestors.
    ///
    /// The iterator will yield the `Path` that is returned if the [`parent`]
    /// method is used zero or more times. That means, the iterator will
    /// yield `&self`, `&self.parent().unwrap()`, `&self.parent().unwrap().
    /// parent().unwrap()` and so on. If the [`parent`] method returns
    /// [`None`], the iterator will do likewise. The iterator will always yield
    /// at least one value, namely `&self`.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::path::Path;
    ///
    /// let mut ancestors = Path::new("/foo/bar").ancestors();
    /// assert_eq!(ancestors.next(), Some(Path::new("/foo/bar")));
    /// assert_eq!(ancestors.next(), Some(Path::new("/foo")));
    /// assert_eq!(ancestors.next(), Some(Path::new("/")));
    /// assert_eq!(ancestors.next(), None);
    /// ```
    ///
    /// [`None`]: ../../std/option/enum.Option.html#variant.None
    /// [`parent`]: struct.Path.html#method.parent
    pub fn ancestors(&self) -> Ancestors<'_> {
        Ancestors { next: Some(self) }
    }

    /// Returns the final component of the `Path`, if there is one.
    ///
    /// If the path is a normal file, this is the file name. If it's the path of
    /// a directory, this is the directory name.
    ///
    /// Returns [`None`] if the path terminates in `..`.
    ///
    /// [`None`]: ../../std/option/enum.Option.html#variant.None
    ///
    /// # Examples
    ///
    /// ```
    /// use std::path::Path;
    /// use std::ffi::str;
    ///
    /// assert_eq!(Some(str::new("bin")), Path::new("/usr/bin/").file_name());
    /// assert_eq!(Some(str::new("foo.txt")), Path::new("tmp/foo.txt").file_name());
    /// assert_eq!(Some(str::new("foo.txt")), Path::new("foo.txt/.").file_name());
    /// assert_eq!(Some(str::new("foo.txt")), Path::new("foo.txt/.//").file_name());
    /// assert_eq!(None, Path::new("foo.txt/..").file_name());
    /// assert_eq!(None, Path::new("/").file_name());
    /// ```
    pub fn file_name(&self) -> Option<&str> {
        self.components().next_back().and_then(|p| match p {
            Component::Normal(p) => Some(p),
            _ => None,
        })
    }

    /// Returns a path that, when joined onto `base`, yields `self`.
    ///
    /// # Errors
    ///
    /// If `base` is not a prefix of `self` (i.e., [`starts_with`]
    /// returns `false`), returns [`Err`].
    ///
    /// [`starts_with`]: #method.starts_with
    /// [`Err`]: ../../std/result/enum.Result.html#variant.Err
    ///
    /// # Examples
    ///
    /// ```
    /// use std::path::{Path, PathBuf};
    ///
    /// let path = Path::new("/test/haha/foo.txt");
    ///
    /// assert_eq!(path.strip_prefix("/"), Ok(Path::new("test/haha/foo.txt")));
    /// assert_eq!(path.strip_prefix("/test"), Ok(Path::new("haha/foo.txt")));
    /// assert_eq!(path.strip_prefix("/test/"), Ok(Path::new("haha/foo.txt")));
    /// assert_eq!(path.strip_prefix("/test/haha/foo.txt"), Ok(Path::new("")));
    /// assert_eq!(path.strip_prefix("/test/haha/foo.txt/"), Ok(Path::new("")));
    /// assert_eq!(path.strip_prefix("test").is_ok(), false);
    /// assert_eq!(path.strip_prefix("/haha").is_ok(), false);
    ///
    /// let prefix = PathBuf::from("/test/");
    /// assert_eq!(path.strip_prefix(prefix), Ok(Path::new("haha/foo.txt")));
    /// ```
    pub fn strip_prefix<P>(&self, base: P) -> Result<&Path, StripPrefixError>
    where
        P: AsRef<Path>,
    {
        self._strip_prefix(base.as_ref())
    }

    fn _strip_prefix(&self, base: &Path) -> Result<&Path, StripPrefixError> {
        iter_after(self.components(), base.components())
            .map(|c| c.as_path())
            .ok_or(StripPrefixError(()))
    }

    /// Determines whether `base` is a prefix of `self`.
    ///
    /// Only considers whole path components to match.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::path::Path;
    ///
    /// let path = Path::new("/etc/passwd");
    ///
    /// assert!(path.starts_with("/etc"));
    /// assert!(path.starts_with("/etc/"));
    /// assert!(path.starts_with("/etc/passwd"));
    /// assert!(path.starts_with("/etc/passwd/"));
    ///
    /// assert!(!path.starts_with("/e"));
    /// ```
    pub fn starts_with<P: AsRef<Path>>(&self, base: P) -> bool {
        self._starts_with(base.as_ref())
    }

    fn _starts_with(&self, base: &Path) -> bool {
        iter_after(self.components(), base.components()).is_some()
    }

    /// Determines whether `child` is a suffix of `self`.
    ///
    /// Only considers whole path components to match.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::path::Path;
    ///
    /// let path = Path::new("/etc/passwd");
    ///
    /// assert!(path.ends_with("passwd"));
    /// ```
    pub fn ends_with<P: AsRef<Path>>(&self, child: P) -> bool {
        self._ends_with(child.as_ref())
    }

    fn _ends_with(&self, child: &Path) -> bool {
        iter_after(self.components().rev(), child.components().rev()).is_some()
    }

    /// Extracts the stem (non-extension) portion of [`self.file_name`].
    ///
    /// [`self.file_name`]: struct.Path.html#method.file_name
    ///
    /// The stem is:
    ///
    /// * [`None`], if there is no file name;
    /// * The entire file name if there is no embedded `.`;
    /// * The entire file name if the file name begins with `.` and has no other
    ///   `.`s within;
    /// * Otherwise, the portion of the file name before the final `.`
    ///
    /// [`None`]: ../../std/option/enum.Option.html#variant.None
    ///
    /// # Examples
    ///
    /// ```
    /// use std::path::Path;
    ///
    /// let path = Path::new("foo.rs");
    ///
    /// assert_eq!("foo", path.file_stem().unwrap());
    /// ```
    pub fn file_stem(&self) -> Option<&str> {
        self.file_name()
            .map(split_file_at_dot)
            .and_then(|(before, after)| before.or(after))
    }

    /// Extracts the extension of [`self.file_name`], if possible.
    ///
    /// The extension is:
    ///
    /// * [`None`], if there is no file name;
    /// * [`None`], if there is no embedded `.`;
    /// * [`None`], if the file name begins with `.` and has no other `.`s
    ///   within;
    /// * Otherwise, the portion of the file name after the final `.`
    ///
    /// [`self.file_name`]: struct.Path.html#method.file_name
    /// [`None`]: ../../std/option/enum.Option.html#variant.None
    ///
    /// # Examples
    ///
    /// ```
    /// use std::path::Path;
    ///
    /// let path = Path::new("foo.rs");
    ///
    /// assert_eq!("rs", path.extension().unwrap());
    /// ```
    pub fn extension(&self) -> Option<&str> {
        self.file_name()
            .map(split_file_at_dot)
            .and_then(|(before, after)| before.and(after))
    }

    /// Creates an owned [`PathBuf`] with `path` adjoined to `self`.
    ///
    /// See [`PathBuf::push`] for more details on what it means to adjoin a
    /// path.
    ///
    /// [`PathBuf`]: struct.PathBuf.html
    /// [`PathBuf::push`]: struct.PathBuf.html#method.push
    ///
    /// # Examples
    ///
    /// ```
    /// use std::path::{Path, PathBuf};
    ///
    /// assert_eq!(Path::new("/etc").join("passwd"), PathBuf::from("/etc/passwd"));
    /// ```
    #[must_use]
    pub fn join<P: AsRef<Path>>(&self, path: P) -> PathBuf {
        self._join(path.as_ref())
    }

    fn _join(&self, path: &Path) -> PathBuf {
        let mut buf = self.to_path_buf();
        buf.push(path);
        buf
    }

    /// Creates an owned [`PathBuf`] like `self` but with the given file name.
    ///
    /// See [`PathBuf::set_file_name`] for more details.
    ///
    /// [`PathBuf`]: struct.PathBuf.html
    /// [`PathBuf::set_file_name`]: struct.PathBuf.html#method.set_file_name
    ///
    /// # Examples
    ///
    /// ```
    /// use std::path::{Path, PathBuf};
    ///
    /// let path = Path::new("/tmp/foo.txt");
    /// assert_eq!(path.with_file_name("bar.txt"), PathBuf::from("/tmp/bar.txt"));
    ///
    /// let path = Path::new("/tmp");
    /// assert_eq!(path.with_file_name("var"), PathBuf::from("/var"));
    /// ```

    pub fn with_file_name<S: AsRef<str>>(&self, file_name: S) -> PathBuf {
        self._with_file_name(file_name.as_ref())
    }

    fn _with_file_name(&self, file_name: &str) -> PathBuf {
        let mut buf = self.to_path_buf();
        buf.set_file_name(file_name);
        buf
    }

    /// Creates an owned [`PathBuf`] like `self` but with the given extension.
    ///
    /// See [`PathBuf::set_extension`] for more details.
    ///
    /// [`PathBuf`]: struct.PathBuf.html
    /// [`PathBuf::set_extension`]: struct.PathBuf.html#method.set_extension
    ///
    /// # Examples
    ///
    /// ```
    /// use std::path::{Path, PathBuf};
    ///
    /// let path = Path::new("foo.rs");
    /// assert_eq!(path.with_extension("txt"), PathBuf::from("foo.txt"));
    /// ```
    pub fn with_extension<S: AsRef<str>>(&self, extension: S) -> PathBuf {
        self._with_extension(extension.as_ref())
    }

    fn _with_extension(&self, extension: &str) -> PathBuf {
        let mut buf = self.to_path_buf();
        buf.set_extension(extension);
        buf
    }

    /// Produces an iterator over the [`Component`]s of the path.
    ///
    /// When parsing the path, there is a small amount of normalization:
    ///
    /// * Repeated separators are ignored, so `a/b` and `a//b` both have `a` and
    ///   `b` as components.
    ///
    /// * Occurrences of `.` are normalized away, except if they are at the
    ///   beginning of the path. For example, `a/./b`, `a/b/`, `a/b/.` and `a/b`
    ///   all have `a` and `b` as components, but `./a/b` starts with an
    ///   additional [`CurDir`] component.
    ///
    /// * A trailing slash is normalized away, `/a/b` and `/a/b/` are
    ///   equivalent.
    ///
    /// Note that no other normalization takes place; in particular, `a/c`
    /// and `a/b/../c` are distinct, to account for the possibility that `b`
    /// is a symbolic link (so its parent isn't `a`).
    ///
    /// # Examples
    ///
    /// ```
    /// use std::path::{Path, Component};
    /// use std::ffi::str;
    ///
    /// let mut components = Path::new("/tmp/foo.txt").components();
    ///
    /// assert_eq!(components.next(), Some(Component::RootDir));
    /// assert_eq!(components.next(), Some(Component::Normal(str::new("tmp"))));
    /// assert_eq!(components.next(), Some(Component::Normal(str::new("foo.txt"))));
    /// assert_eq!(components.next(), None)
    /// ```
    ///
    /// [`Component`]: enum.Component.html
    /// [`CurDir`]: enum.Component.html#variant.CurDir
    pub fn components(&self) -> Components<'_> {
        Components {
            path: self.as_u8_slice(),
            has_physical_root: has_physical_root(self.as_u8_slice()),
            front: State::Prefix,
            back: State::Body,
        }
    }

    /// Produces an iterator over the path's components viewed as [`str`]
    /// slices.
    ///
    /// For more information about the particulars of how the path is separated
    /// into components, see [`components`].
    ///
    /// [`components`]: #method.components
    /// [`str`]: ../ffi/struct.str.html
    ///
    /// # Examples
    ///
    /// ```
    /// use std::path::{self, Path};
    /// use std::ffi::str;
    ///
    /// let mut it = Path::new("/tmp/foo.txt").iter();
    /// assert_eq!(it.next(), Some(str::new(&path::MAIN_SEPARATOR.to_string())));
    /// assert_eq!(it.next(), Some(str::new("tmp")));
    /// assert_eq!(it.next(), Some(str::new("foo.txt")));
    /// assert_eq!(it.next(), None)
    /// ```
    pub fn iter(&self) -> Iter<'_> {
        Iter {
            inner: self.components(),
        }
    }

    /// Returns an object that implements [`Display`] for safely printing paths
    /// that may contain non-Unicode data.
    ///
    /// [`Display`]: ../fmt/trait.Display.html
    ///
    /// # Examples
    ///
    /// ```
    /// use std::path::Path;
    ///
    /// let path = Path::new("/tmp/foo.rs");
    ///
    /// println!("{}", path.display());
    /// ```
    pub fn display(&self) -> Display<'_> {
        Display { path: self }
    }

    /// Converts a [`Box<Path>`][`Box`] into a [`PathBuf`] without copying or
    /// allocating.
    ///
    /// [`Box`]: ../../std/boxed/struct.Box.html
    /// [`PathBuf`]: struct.PathBuf.html
    pub fn into_path_buf(self: Box<Path>) -> PathBuf {
        let rw = Box::into_raw(self) as *mut str;
        let inner = unsafe { Box::from_raw(rw) };
        PathBuf {
            inner: String::from(inner),
        }
    }
}

impl AsRef<str> for Path {
    fn as_ref(&self) -> &str {
        &self.inner
    }
}

impl fmt::Debug for Path {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.inner, formatter)
    }
}

/// Helper struct for safely printing paths with [`format!`] and `{}`.
///
/// A [`Path`] might contain non-Unicode data. This `struct` implements the
/// [`Display`] trait in a way that mitigates that. It is created by the
/// [`display`][`Path::display`] method on [`Path`].
///
/// # Examples
///
/// ```
/// use std::path::Path;
///
/// let path = Path::new("/tmp/foo.rs");
///
/// println!("{}", path.display());
/// ```
///
/// [`Display`]: ../../std/fmt/trait.Display.html
/// [`format!`]: ../../std/macro.format.html
/// [`Path`]: struct.Path.html
/// [`Path::display`]: struct.Path.html#method.display

pub struct Display<'a> {
    path: &'a Path,
}

impl fmt::Debug for Display<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.path, f)
    }
}

impl fmt::Display for Display<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", &self.path.inner)
    }
}

impl cmp::PartialEq for Path {
    fn eq(&self, other: &Path) -> bool {
        self.components().eq(other.components())
    }
}

impl Hash for Path {
    fn hash<H: Hasher>(&self, h: &mut H) {
        for component in self.components() {
            component.hash(h);
        }
    }
}

impl cmp::Eq for Path {}

impl cmp::PartialOrd for Path {
    fn partial_cmp(&self, other: &Path) -> Option<cmp::Ordering> {
        self.components().partial_cmp(other.components())
    }
}

impl cmp::Ord for Path {
    fn cmp(&self, other: &Path) -> cmp::Ordering {
        self.components().cmp(other.components())
    }
}

impl AsRef<Path> for Path {
    fn as_ref(&self) -> &Path {
        self
    }
}

impl AsRef<Path> for str {
    fn as_ref(&self) -> &Path {
        Path::new(self)
    }
}

impl AsRef<Path> for Cow<'_, str> {
    fn as_ref(&self) -> &Path {
        Path::new(self)
    }
}

impl AsRef<Path> for String {
    fn as_ref(&self) -> &Path {
        Path::new(self)
    }
}

impl AsRef<Path> for PathBuf {
    #[inline]
    fn as_ref(&self) -> &Path {
        self
    }
}

impl<'a> IntoIterator for &'a PathBuf {
    type Item = &'a str;
    type IntoIter = Iter<'a>;
    fn into_iter(self) -> Iter<'a> {
        self.iter()
    }
}

impl<'a> IntoIterator for &'a Path {
    type Item = &'a str;
    type IntoIter = Iter<'a>;
    fn into_iter(self) -> Iter<'a> {
        self.iter()
    }
}

macro_rules! impl_cmp {
    ($lhs:ty, $rhs: ty) => {
        impl<'a, 'b> PartialEq<$rhs> for $lhs {
            #[inline]
            fn eq(&self, other: &$rhs) -> bool {
                <Path as PartialEq>::eq(self, other)
            }
        }

        impl<'a, 'b> PartialEq<$lhs> for $rhs {
            #[inline]
            fn eq(&self, other: &$lhs) -> bool {
                <Path as PartialEq>::eq(self, other)
            }
        }

        impl<'a, 'b> PartialOrd<$rhs> for $lhs {
            #[inline]
            fn partial_cmp(&self, other: &$rhs) -> Option<cmp::Ordering> {
                <Path as PartialOrd>::partial_cmp(self, other)
            }
        }

        impl<'a, 'b> PartialOrd<$lhs> for $rhs {
            #[inline]
            fn partial_cmp(&self, other: &$lhs) -> Option<cmp::Ordering> {
                <Path as PartialOrd>::partial_cmp(self, other)
            }
        }
    };
}

impl_cmp!(PathBuf, Path);
impl_cmp!(PathBuf, &'a Path);
impl_cmp!(Cow<'a, Path>, Path);
impl_cmp!(Cow<'a, Path>, &'b Path);
impl_cmp!(Cow<'a, Path>, PathBuf);

macro_rules! impl_cmp_os_str {
    ($lhs:ty, $rhs: ty) => {
        impl<'a, 'b> PartialEq<$rhs> for $lhs {
            #[inline]
            fn eq(&self, other: &$rhs) -> bool {
                <Path as PartialEq>::eq(self, other.as_ref())
            }
        }

        impl<'a, 'b> PartialEq<$lhs> for $rhs {
            #[inline]
            fn eq(&self, other: &$lhs) -> bool {
                <Path as PartialEq>::eq(self.as_ref(), other)
            }
        }

        impl<'a, 'b> PartialOrd<$rhs> for $lhs {
            #[inline]
            fn partial_cmp(&self, other: &$rhs) -> Option<cmp::Ordering> {
                <Path as PartialOrd>::partial_cmp(self, other.as_ref())
            }
        }

        impl<'a, 'b> PartialOrd<$lhs> for $rhs {
            #[inline]
            fn partial_cmp(&self, other: &$lhs) -> Option<cmp::Ordering> {
                <Path as PartialOrd>::partial_cmp(self.as_ref(), other)
            }
        }
    };
}

impl_cmp_os_str!(PathBuf, str);
impl_cmp_os_str!(PathBuf, &'a str);
impl_cmp_os_str!(PathBuf, String);
impl_cmp_os_str!(Path, str);
impl_cmp_os_str!(Path, &'a str);
impl_cmp_os_str!(Path, String);
impl_cmp_os_str!(&'a Path, str);
impl_cmp_os_str!(&'a Path, String);
