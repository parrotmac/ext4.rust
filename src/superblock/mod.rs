mod raw;

use crate::transaction::Transaction;
use crate::{BlockGroupId, Config, FsError, InodeNumber, LogicalBlockNumber, TicketLock};
use alloc::collections::BTreeMap;
use bitflags::bitflags;
use core::sync::atomic::{AtomicU32, Ordering};

pub(crate) use raw::Manipulator;

bitflags! {
    pub struct Ext4FeatureCompatible: u32 {
        const DIR_PREALLOC = 0x0001;
        const IMAGIC_INODES = 0x0002;
        const HAS_JOURNAL = 0x0004;
        const EXT_ATTR = 0x0008;
        const RESIZE_INODE = 0x0010;
        const DIR_INDEX = 0x0020;
    }
}

bitflags! {
    pub struct Ext4FeatureIncompatible: u32 {
        const COMPRESSION = 0x0001;
        const FILETYPE = 0x0002;
        /// Needs recovery
        const RECOVER = 0x0004;
        /// Journal device
        const JOURNAL_DEV = 0x0008;
        const META_BG = 0x0010;
        /// extents support
        const EXTENTS = 0x0040;
        const BIT64 = 0x0080;
        const MMP = 0x0100;
        const FLEX_BG = 0x0200;
        /// EA in inode
        const EA_INODE = 0x0400;
        /// data in dirent
        const DIRDATA = 0x1000;
        /// use crc32c for bg
        const BG_USE_META_CSUM = 0x2000;
        /// >2GB or 3-lvl htree
        const LARGEDIR = 0x4000;
        /// data in inode
        const INLINE_DATA = 0x8000;
    }
}

bitflags! {
    pub struct Ext4FeatureReadOnly: u32 {
        const SPARSE_SUPER = 0x0001;
        const LARGE_FILE = 0x0002;
        const BTREE_DIR = 0x0004;
        const HUGE_FILE = 0x0008;
        const GDT_CSUM = 0x0010;
        const DIR_NLINK = 0x0020;
        const EXTRA_ISIZE = 0x0040;
        const QUOTA = 0x0100;
        const BIGALLOC = 0x0200;
        const METADATA_CSUM = 0x0400;
    }
}

bitflags! {
    pub struct Ext4Flag: u32 {
        const SIGNED_HASH = 0x0001;
        const UNSIGNED_HASH = 0x0002;
        const TEST_FILESYS = 0x0004;
    }
}

/// In-memory superblock.
pub struct SuperBlock<C: Config, const BLK_SIZE: usize> {
    // RO fields
    pub is_dir_entry_version2: bool,
    pub inode_size: usize,
    pub inodes_per_group: u32,
    pub block_desc_size: usize,
    pub first_meta_bg: u32,
    pub features_incompatible: Ext4FeatureIncompatible,
    pub features_compatible: Ext4FeatureCompatible,
    pub features_readonly: Ext4FeatureReadOnly,
    pub first_data_block: u32,
    pub blocks_per_group: u32,
    pub creator_os: u32,
    pub rev_level: u32,
    pub flags: Ext4Flag,
    pub hash_seed: [u32; 4],
    pub blocks_count: u64,
    pub inodes_count: u32,
    pub bg_count: u32,
    pub want_extra_isize: u16,
    pub uuid: [u8; 16usize],
    pub default_hash_version: u8,

    // RW fields.
    free_inodes_count: AtomicU32,
    // inode -> (prev, next)
    pub orphaned_inode: TicketLock<BTreeMap<InodeNumber, (InodeNumber, InodeNumber)>, C::S>,

    pub(crate) manipulator: TicketLock<Manipulator<C, BLK_SIZE>, C::S>,
}

impl<C: Config, const BLK_SIZE: usize> SuperBlock<C, BLK_SIZE> {
    fn verify(self) -> Result<Self, FsError> {
        if self.block_desc_size > BLK_SIZE
            || (self.block_desc_size > 32 && self.block_desc_size < 64)
        {
            Err(FsError::InvalidFs("Invalid Block descriptor size"))
        } else {
            Ok(self)
        }
    }

    pub(crate) fn from_raw(mut manipulator: Manipulator<C, BLK_SIZE>) -> Result<Self, FsError> {
        const EXT4_MIN_BLOCK_GROUP_DESCRIPTOR_SIZE: usize = 32;

        let desc_size = manipulator.desc_size().get() as usize;
        let blocks_count = manipulator.blocks_count().get();
        let bg_count = {
            let blocks_per_group = manipulator.blocks_per_group().get() as u64;
            (blocks_count + blocks_per_group - 1) / blocks_per_group
        } as u32;

        Self {
            is_dir_entry_version2: manipulator.rev_level().get() > 0
                || manipulator.minor_rev_level().get() >= 5,
            inode_size: manipulator.inode_size().get() as usize,
            inodes_per_group: manipulator.inodes_per_group().get(),
            block_desc_size: if desc_size < EXT4_MIN_BLOCK_GROUP_DESCRIPTOR_SIZE {
                EXT4_MIN_BLOCK_GROUP_DESCRIPTOR_SIZE
            } else {
                desc_size
            },
            first_meta_bg: manipulator.first_meta_bg().get(),
            features_incompatible: Ext4FeatureIncompatible::from_bits_truncate(
                manipulator.feature_incompat().get(),
            ),
            features_compatible: Ext4FeatureCompatible::from_bits_truncate(
                manipulator.feature_compat().get(),
            ),
            features_readonly: Ext4FeatureReadOnly::from_bits_truncate(
                manipulator.feature_ro_compat().get(),
            ),
            first_data_block: manipulator.first_data_block().get(),
            blocks_per_group: manipulator.blocks_per_group().get(),
            creator_os: manipulator.creator_os().get(),
            rev_level: manipulator.rev_level().get(),
            flags: Ext4Flag::from_bits_truncate(manipulator.flags().get()),
            hash_seed: [
                manipulator.rw.read_u32(0xEC),
                manipulator.rw.read_u32(0xF0),
                manipulator.rw.read_u32(0xF4),
                manipulator.rw.read_u32(0xF8),
            ],
            blocks_count,
            bg_count,
            inodes_count: manipulator.inodes_count().get(),
            want_extra_isize: manipulator.want_extra_isize().get(),
            uuid: [
                manipulator.rw.read_u8(0x68),
                manipulator.rw.read_u8(0x69),
                manipulator.rw.read_u8(0x6A),
                manipulator.rw.read_u8(0x6B),
                manipulator.rw.read_u8(0x6C),
                manipulator.rw.read_u8(0x6D),
                manipulator.rw.read_u8(0x6E),
                manipulator.rw.read_u8(0x6F),
                manipulator.rw.read_u8(0x70),
                manipulator.rw.read_u8(0x71),
                manipulator.rw.read_u8(0x72),
                manipulator.rw.read_u8(0x73),
                manipulator.rw.read_u8(0x74),
                manipulator.rw.read_u8(0x75),
                manipulator.rw.read_u8(0x76),
                manipulator.rw.read_u8(0x77),
            ],
            default_hash_version: manipulator.default_hash_version().get(),

            free_inodes_count: AtomicU32::new(manipulator.free_inodes_count().get()),
            orphaned_inode: TicketLock::new(BTreeMap::new()),
            manipulator: TicketLock::new(manipulator),
        }
        .verify()
    }

    #[inline]
    pub fn add_orphan_link(&self, pred: InodeNumber, cur: InodeNumber, succ: InodeNumber) {
        self.orphaned_inode.lock().insert(cur, (pred, succ));
    }

    #[inline]
    pub fn replace_orphan_prevlink(&self, pred: InodeNumber, cur: InodeNumber) {
        if let Some((n, _)) = self.orphaned_inode.lock().get_mut(&cur) {
            *n = pred;
        }
    }

    #[inline]
    pub fn remove_orphan(&self, cur: InodeNumber, tx: &Transaction) {
        let mut guard = self.orphaned_inode.lock();
        if let Some((pred, succ)) = guard.remove(&cur) {
            if let Some((_, psucc)) = guard.get_mut(&pred) {
                *psucc = succ;
            }
            if let Some((spred, _)) = guard.get_mut(&succ) {
                *spred = pred;
            }
            tx.inode_update_orphan_link(pred, succ);
        } else {
            // Need to search the disk
            todo!()
        }
    }

    #[inline]
    pub fn get_free_inodes_count(&self) -> u32 {
        self.free_inodes_count.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn inc_free_inodes_count(&self) {
        self.free_inodes_count.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn dec_free_inodes_count(&self, trans: &Transaction) {
        self.free_inodes_count.fetch_sub(1, Ordering::Relaxed);
        trans.free_inodes_count_dec_on_sb();
    }

    #[inline]
    pub fn get_inodes_in_group_cnt(&self, bgid: u32) -> u32 {
        let bg_count = self.bg_count;
        let inodes_per_group = self.inodes_per_group as u32;
        let total_inodes = self.inodes_count;
        if bgid < bg_count - 1 {
            inodes_per_group
        } else {
            total_inodes - ((bg_count - 1) * inodes_per_group)
        }
    }

    #[inline]
    pub fn first_bg_block_no(&self, bgid: BlockGroupId) -> LogicalBlockNumber {
        LogicalBlockNumber(
            bgid.0 as u64 * self.blocks_per_group as u64 + self.first_data_block as u64,
        )
    }

    #[inline]
    fn sb_sparse(&self, grp: u32) -> bool {
        fn is_power_of_n(mut n: u32, v: u32) -> bool {
            use core::cmp::Ordering;

            loop {
                match n.cmp(&v) {
                    _ if n % v != 0 => break false,
                    Ordering::Equal => break true,
                    Ordering::Less => break false,
                    Ordering::Greater => n /= v,
                }
            }
        }

        if !self
            .features_readonly
            .contains(Ext4FeatureReadOnly::SPARSE_SUPER)
        {
            false
        } else if grp <= 1 {
            true
        } else if grp & 1 == 0 {
            false
        } else {
            is_power_of_n(grp, 3) || is_power_of_n(grp, 5) || is_power_of_n(grp, 7)
        }
    }

    #[inline]
    pub fn is_super_in_bg(&self, bgid: BlockGroupId) -> bool {
        !self
            .features_readonly
            .contains(Ext4FeatureReadOnly::SPARSE_SUPER)
            || self.sb_sparse(bgid.0)
    }

    pub fn get_descriptor_block(
        &self,
        bgid: BlockGroupId,
        dsc_per_block: u32,
    ) -> LogicalBlockNumber {
        let dsc_id = bgid.0 / dsc_per_block;
        if !self
            .features_incompatible
            .contains(Ext4FeatureIncompatible::META_BG)
            || dsc_id < self.first_meta_bg
        {
            LogicalBlockNumber(self.first_data_block as u64 + dsc_id as u64 + 1)
        } else if self.is_super_in_bg(bgid) {
            self.first_bg_block_no(bgid) + 1
        } else {
            self.first_bg_block_no(bgid)
        }
    }
}

pub(crate) fn new_sb<C: Config, const N: usize>(c: &C) -> Result<SuperBlock<C, N>, FsError> {
    let mut manipulator = Manipulator::from_disk(c)?;
    let lbs = match N {
        1024 => 0,
        2048 => 1,
        4096 => 2,
        _ => unreachable!(),
    };
    if manipulator.log_block_size().get() != lbs {
        Err(FsError::InvalidFs("Block size is differ from expectation."))
    } else {
        SuperBlock::from_raw(manipulator)
    }
}
