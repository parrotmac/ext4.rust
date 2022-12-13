//! Ticket Lock implementations.
//!
//! This might be diverse into the SpinLock and MutexLock.
use crate::HasConstDefault;
use core::cell::UnsafeCell;
use core::mem::ManuallyDrop;
use core::ops::{Deref, DerefMut, Drop};
use core::sync::atomic::{AtomicU32, AtomicU64, Ordering};

/// Trait that defines the operations on lock.
pub trait Dreamer
where
    Self: Send + Sync + Default,
{
    type HookAux;

    /// Prehook that runs before try acquiring the lock.
    fn prehook(&self) -> Self::HookAux;
    /// Hook that runs after acquire the lock.
    fn acquire_hook(&self, loc: &core::panic::Location<'static>);
    /// Hook that runs after releasing the lock.
    fn release_hook(&self, loc: &core::panic::Location<'static>);
    /// Actions for sleeping.
    fn sleeping(&self, locked: &AtomicU32, ticket: u32);
    /// Actions for waking-up.
    fn waking_up(&self);
}

#[repr(C)]
pub union Ticket {
    // ticket:
    // --------------------------------------------------------------------
    // | p(1 bit) |    next(31 bits)    | p(1 bit) |    owner(31 bits)    |
    // --------------------------------------------------------------------
    // p: Padding
    // next: Next ticket number to be published
    // owner: Ticket number serving now
    ticket_owner: ManuallyDrop<(AtomicU32, AtomicU32)>,
    try_lock: ManuallyDrop<AtomicU64>,
}

/// A mutual exclusion primitive useful for protecting shared data
// #[derive(Debug)]
pub struct TicketLock<T, D>
where
    T: ?Sized + Send,
    D: Dreamer,
{
    ticket: Ticket,
    dreamer: D,
    data: UnsafeCell<T>,
}

/// An RAII implementation of a "scoped lock" of a simple lock. When this
/// structure is dropped (falls out of scope), the lock will be unlocked.
///
/// The data protected by the spin lock can be accessed through this guard via
/// its [`Deref`] and [`DerefMut`] implementations.
///
/// This structure is created by the [`lock`](SimpleLock::lock) and
/// [`try_lock`](SimpleLock::try_lock) methods on [`SimpleLock`].
pub struct TicketLockGuard<'a, T, D>
where
    T: ?Sized + Send,
    T: 'a,
    D: Dreamer,
{
    lock: &'a TicketLock<T, D>,
    data: &'a mut T,
    _h: D::HookAux,
}

impl<T, D> TicketLock<T, D>
where
    T: Send,
    D: Dreamer + HasConstDefault,
{
    /// Creates a new spin lock in an unlocked state ready for use.
    pub const fn new_const(data: T) -> TicketLock<T, D> {
        TicketLock {
            ticket: Ticket {
                try_lock: ManuallyDrop::new(AtomicU64::new(0)),
            },
            dreamer: D::DEFAULT,
            data: UnsafeCell::new(data),
        }
    }
}

impl<T, D> TicketLock<T, D>
where
    T: Send,
    D: Dreamer,
{
    /// Creates a new spin lock in an unlocked state ready for use.
    pub fn new(data: T) -> TicketLock<T, D> {
        TicketLock {
            ticket: Ticket {
                try_lock: ManuallyDrop::new(AtomicU64::new(0)),
            },
            dreamer: D::default(),
            data: UnsafeCell::new(data),
        }
    }

    #[inline]
    #[doc(hidden)]
    fn acquire(&self, ticket: u32) {
        loop {
            self.dreamer
                .sleeping(unsafe { &self.ticket.ticket_owner.0 }, ticket);

            let now = unsafe { self.ticket.ticket_owner.0.load(Ordering::Acquire) };
            if now == ticket {
                break;
            }
        }
    }

    /// Acquires a ticket lock, blocking the current thread until it is able to do
    /// so.
    ///
    /// This function will block the kthread until it is available to acquire
    /// the ticket lock. Upon returning, the thread is the only thread with the
    /// lock held. An RAII guard is returned to allow scoped unlock of the lock.
    /// When the guard goes out of scope, the ticket lock will be unlocked.
    ///
    /// The exact behavior on locking a ticket lock in the thread which already
    /// holds the lock is left unspecified. However, this function will not
    /// return on the second call (it might panic or deadlock, for example).
    #[inline]
    #[track_caller]
    pub fn lock(&self) -> TicketLockGuard<T, D> {
        let h = self.dreamer.prehook();

        let ticket = unsafe { self.ticket.ticket_owner.1.fetch_add(1, Ordering::Relaxed) };

        let now = unsafe { self.ticket.ticket_owner.0.load(Ordering::Relaxed) };
        if now == ticket {
            self.dreamer.acquire_hook(core::panic::Location::caller());
            return TicketLockGuard {
                lock: self,
                data: unsafe { &mut *self.data.get() },
                _h: h,
            };
        }

        self.acquire(ticket);

        self.dreamer.acquire_hook(core::panic::Location::caller());
        TicketLockGuard {
            lock: self,
            data: unsafe { &mut *self.data.get() },
            _h: h,
        }
    }

    /// Attempts to acquire this lock.
    ///
    /// If the lock could not be acquired at this time, then Err is returned.
    /// Otherwise, an RAII guard is returned. The lock will be unlocked when the
    /// guard is dropped.
    ///
    /// This function does not block.
    #[inline]
    #[track_caller]
    pub fn try_lock(&self) -> Result<TicketLockGuard<T, D>, crate::WouldBlock> {
        let h = self.dreamer.prehook();

        let prev = unsafe { self.ticket.ticket_owner.0.load(Ordering::Acquire) } as u64;

        let cmp = (prev << u32::BITS) | prev;
        let new = ((prev + 1) << u32::BITS) | prev;

        if unsafe {
            self.ticket
                .try_lock
                .compare_exchange(cmp, new, Ordering::Acquire, Ordering::Acquire)
        }
        .is_ok()
        {
            self.dreamer.acquire_hook(core::panic::Location::caller());
            Ok(TicketLockGuard {
                lock: self,
                data: unsafe { &mut *self.data.get() },
                _h: h,
            })
        } else {
            Err(crate::WouldBlock)
        }
    }

    /// Takes the mutable reference of the value without checking the value is locked.
    ///
    /// # Safety
    /// This is unsafe.
    #[inline]
    #[allow(clippy::mut_from_ref)]
    pub unsafe fn steal(&self) -> &mut T {
        &mut *self.data.get()
    }
}

unsafe impl<T, D> Sync for TicketLock<T, D>
where
    T: ?Sized + Send,
    D: Dreamer,
{
}
unsafe impl<T, D> Send for TicketLock<T, D>
where
    T: ?Sized + Send,
    D: Dreamer,
{
}

impl<'a, T, D> Deref for TicketLockGuard<'a, T, D>
where
    T: Send + ?Sized,
    D: Dreamer,
{
    type Target = T;
    fn deref(&self) -> &T {
        &*self.data
    }
}

impl<'a, T, D> DerefMut for TicketLockGuard<'a, T, D>
where
    T: Send + ?Sized,
    D: Dreamer,
{
    fn deref_mut(&mut self) -> &mut T {
        &mut *self.data
    }
}

impl<'a, T, D> Drop for TicketLockGuard<'a, T, D>
where
    T: Send + ?Sized,
    D: Dreamer,
{
    #[track_caller]
    fn drop(&mut self) {
        unsafe {
            self.lock
                .ticket
                .ticket_owner
                .0
                .fetch_add(1, Ordering::Relaxed)
        };
        self.lock.dreamer.waking_up();
        self.lock
            .dreamer
            .release_hook(core::panic::Location::caller());
    }
}
