//! Ext4 implementation.
//!
//! <https://ext4.wiki.kernel.org/index.php/Ext4_Disk_Layout>

#![cfg_attr(all(not(feature = "std"), not(test)), no_std)]
#![deny(unsafe_code)]
#![feature(array_chunks, generic_associated_types, min_specialization)]

#[macro_use]
mod prelude;
pub use prelude::*;

extern crate alloc;

#[macro_use]
mod inode;

// Impls.
mod block;
mod block_group;
mod cache;
mod crc;
mod directory;
mod file;
mod filesystem;
mod hasher;
mod superblock;
mod transaction;
mod types;
#[allow(dead_code)]
mod utils;

pub use crate::{directory::Directory, file::File, filesystem::FileSystem};
use alloc::sync::Arc;
pub use fs_core::{FileType, FsError, InodeMode};
pub use inode::{AddressingOutput, Inode};
pub use transaction::Transaction;
pub use types::{
    BlockGroupId, Config, FileBlockNumber, FsObject, InodeNumber, LogicalBlockNumber, Zero,
};

pub enum FsBlkSizeDispatch<C: Config> {
    Blk1024(Arc<FileSystem<C, 1024>>),
    Blk2048(Arc<FileSystem<C, 2048>>),
    Blk4096(Arc<FileSystem<C, 4096>>),
}

/// Open filesystem from io.
pub fn open_fs<C: Config, const N: usize>(conf: C) -> Result<Arc<FileSystem<C, N>>, FsError> {
    superblock::new_sb(&conf).and_then(|sb| FileSystem::new(conf, sb))
}

pub mod format;
