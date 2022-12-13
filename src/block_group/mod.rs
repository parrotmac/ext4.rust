mod raw;

use crate::crc::{Crc16, Crc32c};
use crate::filesystem::FileSystem;
use crate::transaction::Transaction;
use crate::types::BlockGroupId;

use crate::superblock::{Ext4FeatureReadOnly, SuperBlock};
use crate::utils::ByteRw;
use crate::{Config, FileType, FsError, InodeNumber, LogicalBlockNumber};
use bitflags::bitflags;

use alloc::sync::Arc;
pub use core::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
pub(crate) use raw::Manipulator;

bitflags! {
    pub struct BlockGroupFlag: u16 {
        /// Inode table/bitmap not in use
        const INODE_UNINIT = 0x0001;
        /// Block bitmap not in use
        const BLOCK_UNINIT = 0x0002;
        /// On-disk itable initialized to zero
        const ITABLE_ZEROED = 0x0004;
    }
}

#[derive(Debug)]
pub struct BlockGroup<const BLK_SIZE: usize> {
    // Readonly
    pub bgid: BlockGroupId,

    pub inode_table_first_block: LogicalBlockNumber,
    pub block_bitmap_lba: LogicalBlockNumber,
    pub inode_bitmap_lba: LogicalBlockNumber,

    pub blocks_count: u32,

    // Rw
    free_blocks_count: AtomicU32,
    free_inodes_count: AtomicU32,

    // FIXME
    _itable_unused: AtomicU32,
}

impl<const BLK_SIZE: usize> BlockGroup<BLK_SIZE> {
    pub(crate) fn calculate_csum<C: Config, T>(
        bgid: BlockGroupId,
        manipulator: &mut Manipulator<T>,
        sb: &SuperBlock<C, BLK_SIZE>,
    ) -> u16
    where
        T: core::convert::AsRef<[u8]>,
        T: core::convert::AsMut<[u8]>,
    {
        if sb
            .features_readonly
            .contains(Ext4FeatureReadOnly::METADATA_CSUM)
        {
            let orig = manipulator.checksum().get();
            manipulator.checksum().set(0);

            let mut crc = Crc32c::default();
            crc.write(&sb.uuid);
            crc.write(&bgid.0.to_le_bytes());
            crc.write(manipulator.rw.inner().as_ref());

            manipulator.checksum().set(orig);

            crc.finish() as u16
        } else if sb.features_readonly.contains(Ext4FeatureReadOnly::GDT_CSUM) {
            let bytes = manipulator.rw.inner().as_ref();
            let mut crc = Crc16::default();
            crc.write(&sb.uuid);
            crc.write(&bgid.0.to_le_bytes());
            crc.write(&bytes[..0x1E]);
            if bytes.len() > 0x20 {
                crc.write(&bytes[0x20..]);
            }
            crc.finish()
        } else {
            0
        }
    }

    #[inline]
    pub fn allocate_inode_on_bg(&self, ofs: u32, trans: &Transaction, de: FileType) {
        self.free_inodes_count.fetch_sub(1, Ordering::Relaxed);
        trans.inode_allocation_on_bg(self.bgid, self.inode_bitmap_lba, ofs as usize, de);
    }

    #[inline]
    pub fn deallocate_inode(&self) {
        self.free_inodes_count.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn get_free_inodes_count(&self) -> u32 {
        self.free_inodes_count.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn get_free_blocks_count(&self) -> u32 {
        self.free_blocks_count.load(Ordering::Acquire)
    }

    #[inline]
    pub fn allocate_blocks(&self, ino: InodeNumber, ofs: usize, count: usize, trans: &Transaction) {
        self.free_blocks_count
            .fetch_sub(count as u32, Ordering::Release);
        trans.block_allocation_on_bg(ino, self.bgid, self.block_bitmap_lba, ofs, count);
    }

    #[inline]
    pub fn deallocate_blocks(&self, count: usize) {
        self.free_blocks_count
            .fetch_add(count as u32, Ordering::Release);
    }

    pub(crate) fn from_disk<C: Config>(
        raw: crate::block::BlockRef<C, BLK_SIZE, true>,
        split: core::ops::Range<usize>,
        bgid: BlockGroupId,
        fs: &Arc<FileSystem<C, BLK_SIZE>>,
        tx: &Transaction,
    ) -> Result<Self, FsError> {
        let guard = raw.read();
        let mut manipulator = Manipulator::new(&guard.as_ref()[split.clone()]);
        let blocks_count = match bgid.0.cmp(&fs.sb.bg_count) {
            core::cmp::Ordering::Less => fs.sb.blocks_per_group,
            core::cmp::Ordering::Equal => {
                (fs.sb.blocks_count - (fs.sb.blocks_per_group as u64 * (bgid.0 as u64 - 1))) as u32
            }
            core::cmp::Ordering::Greater => {
                panic!("{:?}", FsError::InvalidFs("bgid > sb.bg_count"))
            }
        };
        let (flag, csum) = (
            BlockGroupFlag::from_bits_truncate(manipulator.flags().get()),
            manipulator.checksum().get(),
        );
        let bgroup = BlockGroup {
            bgid,
            inode_table_first_block: LogicalBlockNumber(manipulator.inode_table().get()),
            block_bitmap_lba: LogicalBlockNumber(manipulator.block_bitmap().get()),
            inode_bitmap_lba: LogicalBlockNumber(manipulator.inode_bitmap().get()),
            blocks_count,

            free_blocks_count: AtomicU32::new(manipulator.free_blocks_count().get()),
            free_inodes_count: AtomicU32::new(manipulator.free_inodes_count().get()),

            _itable_unused: AtomicU32::new(manipulator.itable_unused().get()),
        };
        drop(guard);
        bgroup.verify(raw, split, fs, flag, csum, tx)
    }

    fn verify<C: Config>(
        self,
        raw: crate::block::BlockRef<C, BLK_SIZE, true>,
        split: core::ops::Range<usize>,
        fs: &Arc<FileSystem<C, BLK_SIZE>>,
        flags: BlockGroupFlag,
        _csum: u16,
        tx: &Transaction,
    ) -> Result<Self, FsError> {
        let desc_blocks =
            ((fs.sb.block_desc_size * fs.sb.bg_count as usize + BLK_SIZE - 1) / BLK_SIZE) as u64;
        let inode_blocks =
            (((fs.sb.inodes_per_group as usize) * (fs.sb.inode_size as usize) + BLK_SIZE - 1)
                / BLK_SIZE) as u64;

        if flags.contains(BlockGroupFlag::BLOCK_UNINIT) {
            let bref = fs
                .blocks
                .get_mut_noload(self.block_bitmap_lba, &tx.collector)?;
            let mut guard = bref.write();
            let mut block_bitmap = ByteRw::new(guard.as_mut());
            let mut base = (fs.sb.first_data_block as u64 + desc_blocks) as usize + 1;
            if fs.sb.is_super_in_bg(self.bgid) {
                base += 1;
                block_bitmap
                    .set_bitmap(0..1 + fs.sb.first_data_block as usize + desc_blocks as usize);
            }
            // Set block bitmap, inode bitmap, inode table as used block.
            block_bitmap.set_bitmap(base..=base + 1 + inode_blocks as usize);
            let block_bitmap_pad_back = ((self.bgid.0 + 1) as usize
                * fs.sb.blocks_per_group as usize)
                .saturating_sub(fs.sb.blocks_count as usize);
            // Set end of block bitmap. kill 1) unusable 2) padding.
            block_bitmap
                .set_bitmap(fs.sb.blocks_per_group as usize - block_bitmap_pad_back..BLK_SIZE * 8);
        }

        if flags.contains(BlockGroupFlag::INODE_UNINIT) {
            let bref = fs
                .blocks
                .get_mut_noload(self.inode_bitmap_lba, &tx.collector)?;

            let mut guard = bref.write();
            ByteRw::new(guard.as_mut()).set_bitmap(fs.sb.inodes_per_group as usize..BLK_SIZE * 8);
            if !flags.contains(BlockGroupFlag::ITABLE_ZEROED) {
                for lba in (self.inode_table_first_block.0
                    ..self.inode_table_first_block.0 + inode_blocks)
                    .map(LogicalBlockNumber)
                {
                    let _ = fs.blocks.get_mut_noload(lba, &tx.collector)?;
                }
            }
        }
        if !flags.is_empty() {
            let mut guard = raw.write();
            let mut manipulator = Manipulator::new(&mut guard.as_mut()[split]);
            manipulator.flags().set(BlockGroupFlag::empty().bits());
            let csum = BlockGroup::calculate_csum(self.bgid, &mut manipulator, &fs.sb);
            manipulator.checksum().set(csum);
        }
        Ok(self)
        /*
        if self.free_blocks_count.load(Ordering::Acquire)
            == self.blocks_count - self.block_bitmap.count_used()
        {
            Ok(self)
        } else {
            Err(FsError::InvalidFs("Bg is corrupted"))
        }
        */
    }
}
