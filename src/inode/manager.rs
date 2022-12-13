use super::{allocator::Allocator, Inode};
use crate::cache::Cache;
use crate::filesystem::FileSystem;
use crate::superblock::SuperBlock;
use crate::transaction::Transaction;
use crate::{Config, FileType, FsError, InodeNumber};
use alloc::sync::Arc;
use core::sync::atomic::AtomicU8;

pub struct Manager<C: Config, const BLK_SIZE: usize> {
    pub(crate) allocator: Allocator,
    pub(crate) inodes: Cache<InodeNumber, Inode<C, BLK_SIZE>, C::D>,
}

type InodeAllocationResult<C, const BLK_SIZE: usize> = (InodeNumber, Arc<Inode<C, BLK_SIZE>>);

impl<C: Config, const BLK_SIZE: usize> Manager<C, BLK_SIZE> {
    #[inline]
    pub fn new(sb: &SuperBlock<C, BLK_SIZE>) -> Self {
        Self {
            allocator: Allocator::new(sb.bg_count as usize),
            inodes: Cache::new(),
        }
    }

    pub(crate) fn load_bitmap(
        &mut self,
        bgs: &[crate::block_group::BlockGroup<BLK_SIZE>],
        blocks: &mut crate::block::Manager<C, BLK_SIZE>,
    ) -> Result<(), FsError> {
        for bg in bgs {
            let bblock = blocks.get(bg.inode_bitmap_lba)?;
            self.allocator
                .bitmap
                .push(bblock.read().iter().cloned().map(AtomicU8::new).collect());
        }
        Ok(())
    }

    pub fn allocate(
        &self,
        fs: &Arc<FileSystem<C, BLK_SIZE>>,
        de: FileType,
        tx: &Transaction,
    ) -> Result<InodeAllocationResult<C, BLK_SIZE>, FsError> {
        let ino = self.allocator.allocate(fs, de, tx)?;

        let mut created = false;
        let v = self
            .inodes
            .get_or_insert::<_, ()>(ino, |ino| {
                created = true;
                Ok(Inode::new(fs, ino, de))
            })
            .unwrap();
        if created {
            Ok((ino, v))
        } else {
            Err(FsError::InvalidFs("Inode object is not cleaned up"))
        }
    }

    pub fn deallocate(
        &self,
        fs: &Arc<FileSystem<C, BLK_SIZE>>,
        ino: InodeNumber,
        ftype: FileType,
        tx: &Transaction,
    ) {
        self.allocator.deallocate(fs, ino, ftype, tx)
    }

    pub fn remove(&self, ino: InodeNumber) {
        self.inodes.take(&ino).unwrap();
    }

    pub fn get(
        &self,
        fs: &Arc<FileSystem<C, BLK_SIZE>>,
        ino: InodeNumber,
        hint: Option<FileType>,
    ) -> Result<Arc<Inode<C, BLK_SIZE>>, FsError> {
        self.inodes.get_or_insert(ino, |ino| {
            let inode_size = fs.sb.inode_size;
            let (block_group, index_in_grp) = ino.into_bgid_index(&fs.sb);
            // Load block group, where i-node is located
            let inode_table_start = fs.get_block_group(block_group).inode_table_first_block;

            // Compute position of i-node in the block group
            let byte_offset_in_group = index_in_grp as u64 * inode_size as u64;
            // Compute block address
            let lba = inode_table_start + (byte_offset_in_group as u64 / BLK_SIZE as u64);
            // Compute position of i-node in the data block
            let offset_in_block = (byte_offset_in_group as usize) % BLK_SIZE;

            let inode_arr = fs.blocks.get(lba)?;
            let guard = inode_arr.read();

            Inode::from_bytes(
                &guard[offset_in_block..offset_in_block + inode_size],
                fs,
                ino,
                hint,
            )
        })
    }

    pub(crate) fn apply_deallocation_on_core(
        &self,
        ino: InodeNumber,
        fs: &FileSystem<C, BLK_SIZE>,
    ) {
        let (bgid, ofs) = ino.into_bgid_index(&fs.sb);
        let bg = fs.get_block_group(bgid);
        let (group, mask) = (ofs >> 3, 1 << (ofs & 7));
        assert_eq!(
            self.allocator.bitmap[bgid.0 as usize][group]
                .fetch_and(!mask, core::sync::atomic::Ordering::Relaxed)
                & mask,
            mask
        );
        fs.sb.inc_free_inodes_count();
        bg.deallocate_inode();
    }
}
