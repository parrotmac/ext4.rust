//! RwLock implementations.
use crate::HasConstDefault;
use core::cell::UnsafeCell;
use core::ops::{Deref, DerefMut, Drop};
use core::sync::atomic::{AtomicUsize, Ordering};

pub enum Hint {
    Read,
    Write,
}

/// Trait that defines the operations on lock.
pub trait Dreamer
where
    Self: Send + Sync + Default,
{
    type HookAux;

    /// Prehook that runs before try acquiring the lock.
    fn prehook(&self) -> Self::HookAux;
    /// Hook that runs after acquire the lock.
    fn acquire_hook(&self, hint: Hint, _l: &core::panic::Location<'static>);
    /// Hook that runs after releasing the lock.
    fn release_hook(&self, hint: Hint, _l: &core::panic::Location<'static>);
    /// Actions for sleeping.
    fn sleeping(&self, locked: &AtomicUsize, hint: Hint);
    /// Actions for waking-up.
    fn waking_up(&self);
}

/// A reader-writer lock
///
/// This type of lock allows a number of readers or at most one writer at any
/// point in time. The write portion of this lock typically allows modification
/// of the underlying data (exclusive access) and the read portion of this lock
/// typically allows for read-only access (shared access).
///
/// In comparison, a [`Mutex`] does not distinguish between readers or writers
/// that acquire the lock, therefore blocking any threads waiting for the lock
/// to become available. An `RwLock` will allow any number of readers to acquire
/// the lock as long as a writer is not holding the lock.
///
/// The priority policy of the lock is dependent on the underlying operating
/// system's implementation, and this type does not guarantee that any
/// particular policy will be used.
///
/// The type parameter `T` represents the data that this lock protects. It is
/// required that `T` satisfies [`Send`] to be shared across threads and
/// [`Sync`] to allow concurrent access through readers. The RAII guards
/// returned from the locking methods implement [`Deref`] (and [`DerefMut`]
/// for the `write` methods) to allow access to the content of the lock.
///
/// [`Mutex`]: struct.Mutex.html
pub struct RwLock<T, D>
where
    T: ?Sized + Send,
    D: Dreamer,
{
    // state:
    // Upper 2bit represent the lock state.
    // 0: Nobody try to get lock for writing.
    // 1: Writer is waiting.
    // 2: Writer holds the lock.
    state: AtomicUsize,
    dreamer: D,
    data: UnsafeCell<T>,
}

const STATE_MASK: usize = 0b1 << (usize::BITS - 2);
const STATE_WRITER_LOCKED: usize = 0b1 << (usize::BITS - 2);

#[inline]
pub fn is_write_locked(b: usize) -> bool {
    b & STATE_MASK == STATE_WRITER_LOCKED
}

#[inline]
pub fn is_read_locked(b: usize) -> bool {
    b > 0 && !is_write_locked(b)
}

/// RAII structure used to release the exclusive write access of a lock when
/// dropped.
///
/// This structure is created by the [`write`] and [`try_write`] methods
/// on [`RwLock`].
///
/// [`write`]: struct.RwLock.html#method.write
/// [`try_write`]: struct.RwLock.html#method.try_write
/// [`RwLock`]: struct.RwLock.html
pub struct RwLockWriteGuard<'a, T, D>
where
    T: ?Sized + Send,
    T: 'a,
    D: Dreamer,
{
    lock: &'a RwLock<T, D>,
    data: &'a mut T,
    _h: D::HookAux,
}

/// RAII structure used to release the shared read access of a lock when
/// dropped.
///
/// This structure is created by the [`read`] and [`try_read`] methods on
/// [`RwLock`].
///
/// [`read`]: struct.RwLock.html#method.read
/// [`try_read`]: struct.RwLock.html#method.try_read
/// [`RwLock`]: struct.RwLock.html
pub struct RwLockReadGuard<'a, T, D>
where
    T: ?Sized + Send,
    T: 'a,
    D: Dreamer,
{
    lock: &'a RwLock<T, D>,
    data: &'a T,
    _h: D::HookAux,
}

impl<'a, T, D> RwLockReadGuard<'a, T, D>
where
    T: ?Sized + Send,
    T: 'a,
    D: Dreamer,
{
    pub fn upgrade(self) -> RwLockWriteGuard<'a, T, D> {
        let this = core::mem::ManuallyDrop::new(self);
        let _h = unsafe { core::ptr::read(&this._h) };
        let lock = unsafe { core::ptr::read(&this.lock) };
        loop {
            if lock
                .state
                .compare_exchange(1, STATE_WRITER_LOCKED, Ordering::Acquire, Ordering::Acquire)
                .is_ok()
            {
                lock.dreamer
                    .release_hook(Hint::Read, core::panic::Location::caller());
                lock.dreamer
                    .acquire_hook(Hint::Write, core::panic::Location::caller());
                break RwLockWriteGuard {
                    lock,
                    data: unsafe { &mut *lock.data.get() },
                    _h,
                };
            }
        }
    }
}

impl<'a, T, D> RwLockWriteGuard<'a, T, D>
where
    T: ?Sized + Send,
    T: 'a,
    D: Dreamer,
{
    pub fn downgrade(self) -> RwLockReadGuard<'a, T, D> {
        let this = core::mem::ManuallyDrop::new(self);
        let _h = unsafe { core::ptr::read(&this._h) };
        let lock = unsafe { core::ptr::read(&this.lock) };
        assert!(lock
            .state
            .compare_exchange(STATE_WRITER_LOCKED, 1, Ordering::Acquire, Ordering::Acquire)
            .is_ok());
        lock.dreamer
            .release_hook(Hint::Write, core::panic::Location::caller());
        lock.dreamer
            .acquire_hook(Hint::Read, core::panic::Location::caller());
        RwLockReadGuard {
            lock,
            data: unsafe { &*lock.data.get() },
            _h,
        }
    }
}

impl<T, D> RwLock<T, D>
where
    T: Send,
    D: Dreamer + HasConstDefault,
{
    /// Creates a new instance of an `RwLock<T>` which is unlocked.
    pub const fn new_const(data: T) -> RwLock<T, D> {
        RwLock {
            state: AtomicUsize::new(0),
            dreamer: D::DEFAULT,
            data: UnsafeCell::new(data),
        }
    }
}

impl<T, D> RwLock<T, D>
where
    T: Send,
    D: Dreamer,
{
    /// Creates a new instance of an `RwLock<T>` which is unlocked.
    pub fn new(data: T) -> RwLock<T, D> {
        RwLock {
            state: AtomicUsize::new(0),
            dreamer: D::default(),
            data: UnsafeCell::new(data),
        }
    }

    #[inline]
    fn read_lock(&self) -> D::HookAux {
        loop {
            let h = self.dreamer.prehook();
            let prev = self.state.load(Ordering::Relaxed);
            if is_write_locked(prev) {
                self.dreamer.sleeping(&self.state, Hint::Read);
            } else if self
                .state
                .compare_exchange(prev, prev + 1, Ordering::Acquire, Ordering::Acquire)
                .is_ok()
            {
                break h;
            }
        }
    }

    /// Locks this rwlock with shared read access, blocking the current thread
    /// until it can be acquired.
    ///
    /// The calling thread will be blocked until there are no more writers which
    /// hold the lock. There may be other readers currently inside the lock when
    /// this method returns. This method does not provide any guarantees with
    /// respect to the ordering of whether contentious readers or writers will
    /// acquire the lock first.
    ///
    /// Returns an RAII guard which will release this thread's shared access
    /// once it is dropped.
    #[inline]
    #[track_caller]
    pub fn read(&self) -> RwLockReadGuard<T, D> {
        if let Ok(guard) = self.try_read() {
            guard
        } else {
            let h = self.read_lock();
            self.dreamer
                .acquire_hook(Hint::Read, core::panic::Location::caller());
            RwLockReadGuard {
                lock: self,
                data: unsafe { &*self.data.get() },
                _h: h,
            }
        }
    }

    /// Attempts to acquire this rwlock with shared read access.
    ///
    /// If the access could not be granted at this time, then `Err` is returned.
    /// Otherwise, an RAII guard is returned which will release the shared
    /// access when it is dropped.
    ///
    /// This function does not block.
    ///
    /// This function does not provide any guarantees with respect to the
    /// ordering of whether contentious readers or writers will acquire the
    /// lock first.
    #[inline]
    #[track_caller]
    pub fn try_read(&self) -> Result<RwLockReadGuard<T, D>, crate::WouldBlock> {
        loop {
            let h = self.dreamer.prehook();
            let prev = self.state.load(Ordering::Relaxed);
            if is_write_locked(prev) {
                break Err(crate::WouldBlock);
            } else if self
                .state
                .compare_exchange(prev, prev + 1, Ordering::Acquire, Ordering::Acquire)
                .is_ok()
            {
                self.dreamer
                    .acquire_hook(Hint::Read, core::panic::Location::caller());
                break Ok(RwLockReadGuard {
                    lock: self,
                    data: unsafe { &*self.data.get() },
                    _h: h,
                });
            }
        }
    }

    #[inline]
    fn write_lock(&self) -> D::HookAux {
        loop {
            let h = self.dreamer.prehook();
            let prev = self.state.load(Ordering::Relaxed);
            if prev > 0 {
                self.dreamer.sleeping(&self.state, Hint::Write);
            } else if self
                .state
                .compare_exchange(0, STATE_WRITER_LOCKED, Ordering::Acquire, Ordering::Acquire)
                .is_ok()
            {
                break h;
            }
        }
    }

    /// Locks this rwlock with exclusive write access, blocking the current
    /// thread until it can be acquired.
    ///
    /// This function will not return while other writers or other readers
    /// currently have access to the lock.
    ///
    /// Returns an RAII guard which will drop the write access of this rwlock
    /// when dropped.
    #[inline]
    #[track_caller]
    pub fn write(&self) -> RwLockWriteGuard<T, D> {
        if let Ok(guard) = self.try_write() {
            guard
        } else {
            let h = self.write_lock();
            self.dreamer
                .acquire_hook(Hint::Write, core::panic::Location::caller());
            RwLockWriteGuard {
                lock: self,
                data: unsafe { &mut *self.data.get() },
                _h: h,
            }
        }
    }

    /// Attempts to lock this rwlock with exclusive write access.
    ///
    /// If the lock could not be acquired at this time, then `Err` is returned.
    /// Otherwise, an RAII guard is returned which will release the lock when
    /// it is dropped.
    ///
    /// This function does not block.
    ///
    /// This function does not provide any guarantees with respect to the
    /// ordering of whether contentious readers or writers will acquire the
    /// lock first.
    #[track_caller]
    pub fn try_write(&self) -> Result<RwLockWriteGuard<T, D>, crate::WouldBlock> {
        loop {
            let h = self.dreamer.prehook();
            let prev = self.state.load(Ordering::Relaxed);
            if prev > 0 {
                break Err(crate::WouldBlock);
            } else if self
                .state
                .compare_exchange(
                    prev,
                    prev | STATE_WRITER_LOCKED,
                    Ordering::Acquire,
                    Ordering::Acquire,
                )
                .is_ok()
            {
                self.dreamer
                    .acquire_hook(Hint::Write, core::panic::Location::caller());
                break Ok(RwLockWriteGuard {
                    lock: self,
                    data: unsafe { &mut *self.data.get() },
                    _h: h,
                });
            }
        }
    }

    /// This steals the ownership even if the value is locked. Racy.
    ///
    /// # Safety
    /// This is unsafe.
    #[inline]
    #[allow(clippy::mut_from_ref)]
    pub unsafe fn steal(&self) -> &mut T {
        &mut *self.data.get()
    }
}

unsafe impl<T, D> Sync for RwLock<T, D>
where
    T: ?Sized + Send,
    D: Dreamer,
{
}
unsafe impl<T, D> Send for RwLock<T, D>
where
    T: ?Sized + Send,
    D: Dreamer,
{
}

impl<'a, T, D> Deref for RwLockReadGuard<'a, T, D>
where
    T: ?Sized + Send,
    D: Dreamer,
{
    type Target = T;
    fn deref(&self) -> &T {
        &*self.data
    }
}

impl<'a, T, D> Deref for RwLockWriteGuard<'a, T, D>
where
    T: ?Sized + Send,
    D: Dreamer,
{
    type Target = T;
    fn deref(&self) -> &T {
        &*self.data
    }
}

impl<'a, T, D> DerefMut for RwLockWriteGuard<'a, T, D>
where
    T: ?Sized + Send,
    D: Dreamer,
{
    fn deref_mut(&mut self) -> &mut T {
        &mut *self.data
    }
}

impl<'a, T, D> Drop for RwLockReadGuard<'a, T, D>
where
    T: ?Sized + Send,
    D: Dreamer,
{
    #[track_caller]
    fn drop(&mut self) {
        debug_assert_eq!(self.lock.state.load(Ordering::Acquire) & STATE_MASK, 0);
        if self.lock.state.fetch_sub(1, Ordering::Release) == 1 {
            self.lock.dreamer.waking_up();
        }
        self.lock
            .dreamer
            .release_hook(Hint::Read, core::panic::Location::caller());
    }
}

impl<'a, T, D> Drop for RwLockWriteGuard<'a, T, D>
where
    T: ?Sized + Send,
    D: Dreamer,
{
    #[track_caller]
    fn drop(&mut self) {
        debug_assert_eq!(
            self.lock.state.load(Ordering::Acquire) & STATE_MASK,
            STATE_WRITER_LOCKED
        );
        self.lock
            .state
            .fetch_and(!STATE_WRITER_LOCKED, Ordering::Release);
        self.lock.dreamer.waking_up();
        self.lock
            .dreamer
            .release_hook(Hint::Write, core::panic::Location::caller());
    }
}
