// Making permanant change.
mod collector;
mod event;

use crate::filesystem::FileSystem;
use crate::inode::RawInodeAddressingMode;
use crate::{BlockGroupId, Config, FileType, FsError, InodeNumber, LogicalBlockNumber};
use alloc::collections::LinkedList;
use alloc::sync::Arc;
use core::cell::RefCell;
use event::{
    BlockAllocationOnBg, BlockDeallocationOnBg, InodeAddLink, InodeAllocationOnBg,
    InodeDeallocationOnBg, InodeRmLink, InodeSetSize, InodeUpdateOrphanLink,
};

pub use collector::Collector;
pub use event::{Event, Events};

/// Per-operation private data.
pub struct Transaction {
    pub(crate) events: Events,
    pub(crate) collector: Collector,
}

// Remove default.
impl Default for Transaction {
    fn default() -> Self {
        Self {
            events: Events {
                inner: Some(RefCell::new(LinkedList::new())),
            },
            collector: Collector::default(),
        }
    }
}

impl Drop for Events {
    #[track_caller]
    fn drop(&mut self) {
        if let Some(events) = self.inner.take() {
            for _ev in events.into_inner() {
                #[cfg(not(test))]
                todo!("handle {:?}", _ev);
            }
        }
    }
}

impl Transaction {
    #[inline]
    pub fn block_allocation_on_bg(
        &self,
        ino: InodeNumber,
        bgid: BlockGroupId,
        bitmap_lba: LogicalBlockNumber,
        ofs: usize,
        count: usize,
    ) {
        self.events
            .inner
            .as_ref()
            .unwrap()
            .borrow_mut()
            .push_back(Event::BlockAllocationOnBg(BlockAllocationOnBg {
                ino,
                bgid,
                bitmap_lba,
                ofs,
                count,
            }))
    }

    #[inline]
    pub fn block_deallocation_on_bg(
        &self,
        ino: InodeNumber,
        bgid: BlockGroupId,
        bitmap_lba: LogicalBlockNumber,
        ofs: usize,
        count: usize,
    ) {
        self.events
            .inner
            .as_ref()
            .unwrap()
            .borrow_mut()
            .push_back(Event::BlockDeallocationOnBg(BlockDeallocationOnBg {
                ino,
                bgid,
                bitmap_lba,
                ofs,
                count,
            }))
    }

    #[inline]
    pub fn inode_allocation_on_bg(
        &self,
        bgid: BlockGroupId,
        bitmap_lba: LogicalBlockNumber,
        ofs: usize,
        de: FileType,
    ) {
        self.events
            .inner
            .as_ref()
            .unwrap()
            .borrow_mut()
            .push_back(Event::InodeAllocationOnBg(InodeAllocationOnBg {
                bgid,
                bitmap_lba,
                ofs,
                de,
            }))
    }

    #[inline]
    pub fn inode_deallocation_on_bg(
        &self,
        ino: InodeNumber,
        bgid: BlockGroupId,
        bitmap_lba: LogicalBlockNumber,
        ofs: usize,
        de: FileType,
    ) {
        self.events
            .inner
            .as_ref()
            .unwrap()
            .borrow_mut()
            .push_back(Event::InodeDeallocationOnBg(InodeDeallocationOnBg {
                ino,
                bgid,
                bitmap_lba,
                ofs,
                de,
            }))
    }

    #[inline]
    pub fn free_inodes_count_dec_on_sb(&self) {
        self.events
            .inner
            .as_ref()
            .unwrap()
            .borrow_mut()
            .push_back(Event::FreeInodesCountDecOnSb);
    }

    #[inline]
    pub fn free_inodes_count_inc_on_sb(&self) {
        self.events
            .inner
            .as_ref()
            .unwrap()
            .borrow_mut()
            .push_back(Event::FreeInodesCountIncOnSb);
    }

    #[inline]
    pub fn inode_set_size(&self, ino: InodeNumber, size: u64, address: RawInodeAddressingMode) {
        self.events
            .inner
            .as_ref()
            .unwrap()
            .borrow_mut()
            .push_back(Event::InodeSetSize(InodeSetSize { ino, size, address }));
    }

    #[inline]
    pub fn inode_add_link(&self, ino: InodeNumber) {
        self.events
            .inner
            .as_ref()
            .unwrap()
            .borrow_mut()
            .push_back(Event::InodeAddLink(InodeAddLink { ino }));
    }

    #[inline]
    pub fn inode_rm_link(&self, ino: InodeNumber) {
        self.events
            .inner
            .as_ref()
            .unwrap()
            .borrow_mut()
            .push_back(Event::InodeRmLink(InodeRmLink { ino }))
    }

    #[inline]
    pub fn inode_update_orphan_link(&self, pred: InodeNumber, succ: InodeNumber) {
        self.events
            .inner
            .as_ref()
            .unwrap()
            .borrow_mut()
            .push_back(Event::InodeUpdateOrphanLink(InodeUpdateOrphanLink {
                pred,
                succ,
            }))
    }

    #[inline]
    pub fn done<C: Config, const BLK_SIZE: usize>(
        self,
        fs: &Arc<FileSystem<C, BLK_SIZE>>,
    ) -> Result<(), FsError> {
        let Self {
            mut events,
            collector,
        } = self;
        // XXX: For now, flush the request right here.
        // XXX: If journaled, use other ways.
        events.apply_on_disk(fs, &collector)?;
        collector.done(fs)
    }
}
