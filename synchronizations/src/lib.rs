//! no_std generic implementations of the synchromization primitives.
#![no_std]
#![feature(const_fn_trait_bound)]

pub mod rwlock;
pub mod simple_lock;
pub mod ticket_lock;

/// Traits for supporting constant creation of the lock.
pub trait HasConstDefault {
    /// Default value.
    const DEFAULT: Self;
}

/// The lock could not be acquired at this time because the operation would
/// otherwise block.
#[derive(Debug)]
pub struct WouldBlock;
