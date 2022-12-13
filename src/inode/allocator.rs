use crate::filesystem::FileSystem;
use crate::transaction::Transaction;
use crate::types::BlockGroupId;
use crate::{Config, FileType, FsError, InodeNumber};
use alloc::boxed::Box;
use alloc::vec::Vec;
use core::sync::atomic::{AtomicU32, AtomicU8, Ordering};

/// Allocator
pub(crate) struct Allocator {
    last_inode_bg_id: AtomicU32,
    pub(super) bitmap: Vec<Box<[AtomicU8]>>,
}

impl Allocator {
    pub fn new(bg: usize) -> Self {
        Self {
            last_inode_bg_id: AtomicU32::new(0),
            bitmap: Vec::with_capacity(bg),
        }
    }

    pub fn try_allocate_at<C: Config, const BLK_SIZE: usize>(
        &self,
        i: u32,
        fs: &FileSystem<C, BLK_SIZE>,
    ) -> Option<InodeNumber> {
        let ino = InodeNumber(i);
        let (bgid, idx) = ino.into_bgid_index(&fs.sb);
        let (group, ofs) = (idx / 8, idx & 7);

        if self.bitmap[bgid.0 as usize][group].fetch_or(1 << ofs, Ordering::Relaxed) & (1 << ofs)
            == 0
        {
            Some(ino)
        } else {
            None
        }
    }

    pub fn get_free_ino(&self, bgid: BlockGroupId) -> Option<usize> {
        for (group, bits) in self.bitmap[bgid.0 as usize].iter().enumerate() {
            loop {
                // CAS to get bits.
                let val = bits.load(Ordering::Relaxed);
                let x = val ^ core::u8::MAX;
                if x != 0 {
                    // toggle all bits.
                    let (mask, ret) = {
                        let pos = 7 - (x & !(x - 1)).leading_zeros() as usize;
                        //println!(
                        //    "{:08b} {}\n{:08b}",
                        //    val,
                        //    pos + ofs * 8,
                        //    1 << pos,
                        //);
                        (1 << pos, group * 8 + pos)
                    };
                    // Check whether previous value does not hold the one on the position.
                    if bits.fetch_or(mask, Ordering::Relaxed) & mask == 0 {
                        return Some(ret);
                    }
                } else {
                    break;
                }
            }
        }
        None
    }

    pub fn allocate<C: Config, const BLK_SIZE: usize>(
        &self,
        fs: &FileSystem<C, BLK_SIZE>,
        de: FileType,
        trans: &Transaction,
    ) -> Result<InodeNumber, FsError> {
        if fs.sb.get_free_inodes_count() == 0 {
            return Err(FsError::FsFull);
        }

        let bgid = self.last_inode_bg_id.load(Ordering::Acquire);
        for bgid in (0..fs.sb.bg_count)
            .map(BlockGroupId)
            .cycle()
            .skip(bgid as usize)
            .take(fs.sb.bg_count as usize)
        {
            let bg = fs.get_block_group(bgid);
            if bg.get_free_inodes_count() > 0 {
                if let Some(ofs) = self.get_free_ino(bgid) {
                    bg.allocate_inode_on_bg(ofs as u32, trans, de);
                    fs.sb.dec_free_inodes_count(trans);
                    self.last_inode_bg_id.store(bgid.0, Ordering::Release);

                    return Ok(InodeNumber::from_bgid_index(bgid, ofs, &fs.sb));
                }
            }
        }

        Err(FsError::FsFull)
    }

    pub fn deallocate<C: Config, const BLK_SIZE: usize>(
        &self,
        fs: &FileSystem<C, BLK_SIZE>,
        ino: InodeNumber,
        de: FileType,
        trans: &Transaction,
    ) {
        let (bgid, ofs) = ino.into_bgid_index(&fs.sb);
        let bg = fs.get_block_group(bgid);
        trans.inode_deallocation_on_bg(ino, bgid, bg.inode_bitmap_lba, ofs as usize, de);
        trans.free_inodes_count_inc_on_sb();
    }
}
