use super::collector::{Collector, PostludeOps};
use crate::block::BlockRef;
use crate::block_group;
use crate::filesystem::FileSystem;
use crate::inode::{self, RawInodeAddressingMode};
use crate::superblock;
use crate::{
    BlockGroupId, Config, FileType, FsError, InodeNumber, LogicalBlockNumber, TicketLockGuard,
};
use alloc::collections::LinkedList;
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::cell::RefCell;
use core::convert::TryInto;
use hashbrown::HashMap;

#[derive(Debug)]
pub struct InodeAllocationOnBg {
    pub(super) bgid: BlockGroupId,
    pub(super) bitmap_lba: LogicalBlockNumber,
    pub(super) ofs: usize,
    pub(super) de: FileType,
}

#[derive(Debug)]
pub struct InodeDeallocationOnBg {
    pub(super) ino: InodeNumber,
    pub(super) bgid: BlockGroupId,
    pub(super) bitmap_lba: LogicalBlockNumber,
    pub(super) ofs: usize,
    pub(super) de: FileType,
}

#[derive(Debug)]
pub struct BlockAllocationOnBg {
    pub(super) ino: InodeNumber,
    pub(super) bgid: BlockGroupId,
    pub(super) bitmap_lba: LogicalBlockNumber,
    pub(super) ofs: usize,
    pub(super) count: usize,
}

#[derive(Debug)]
pub struct BlockDeallocationOnBg {
    pub(super) ino: InodeNumber,
    pub(super) bgid: BlockGroupId,
    pub(super) bitmap_lba: LogicalBlockNumber,
    pub(super) ofs: usize,
    pub(super) count: usize,
}

#[derive(Debug)]
pub struct InodeAddLink {
    pub(super) ino: InodeNumber,
}

#[derive(Debug)]
pub struct InodeRmLink {
    pub(super) ino: InodeNumber,
}

#[derive(Debug)]
pub struct InodeUpdateOrphanLink {
    pub(super) pred: InodeNumber,
    pub(super) succ: InodeNumber,
}

pub struct InodeSetSize {
    pub(super) ino: InodeNumber,
    pub(super) size: u64,
    pub(super) address: RawInodeAddressingMode,
}

impl core::fmt::Debug for InodeSetSize {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> Result<(), core::fmt::Error> {
        f.debug_struct("InodeSetSize")
            .field("ino", &self.ino)
            .field("size", &self.size)
            .finish()
    }
}

#[derive(Debug)]
pub enum Event {
    BlockAllocationOnBg(BlockAllocationOnBg),
    BlockDeallocationOnBg(BlockDeallocationOnBg),
    InodeAllocationOnBg(InodeAllocationOnBg),
    InodeDeallocationOnBg(InodeDeallocationOnBg),
    InodeSetSize(InodeSetSize),
    InodeAddLink(InodeAddLink),
    InodeRmLink(InodeRmLink),
    InodeUpdateOrphanLink(InodeUpdateOrphanLink),
    FreeInodesCountDecOnSb,
    FreeInodesCountIncOnSb,
}

pub struct Events {
    pub(crate) inner: Option<RefCell<LinkedList<Event>>>,
}

impl Events {
    #[inline(never)]
    fn compress<C: Config, const BLK_SIZE: usize>(
        events: LinkedList<Event>,
        collector: &Collector,
        fs: &FileSystem<C, BLK_SIZE>,
    ) -> WritebackGroup<C, BLK_SIZE> {
        let mut wb_grp = WritebackGroup::new();

        for log in events.into_iter() {
            match log {
                Event::BlockAllocationOnBg(BlockAllocationOnBg {
                    bgid,
                    ino,
                    bitmap_lba,
                    ofs,
                    count,
                }) => {
                    wb_grp.bg_delta(bgid).free_blocks_cnt_delta -= count as i64;
                    wb_grp.sb_delta().free_blocks_cnt_delta -= count as i64;
                    wb_grp.inode_ops.push(InodeOps::SetBlock {
                        ino,
                        cnt: count as i64,
                    });
                    wb_grp.set_bitmap(bitmap_lba, ofs, count, collector, fs);
                }
                Event::BlockDeallocationOnBg(BlockDeallocationOnBg {
                    bgid,
                    ino,
                    bitmap_lba,
                    ofs,
                    count,
                }) => {
                    wb_grp.bg_delta(bgid).free_blocks_cnt_delta += count as i64;
                    wb_grp.sb_delta().free_blocks_cnt_delta += count as i64;
                    wb_grp.inode_ops.push(InodeOps::SetBlock { ino, cnt: -1 });
                    wb_grp.unset_bitmap(bitmap_lba, ofs, count, collector, fs);
                    collector
                        .postludes
                        .borrow_mut()
                        .push(PostludeOps::BlockDeallocation { bgid, ofs, count });
                }
                Event::InodeUpdateOrphanLink(log) => {
                    wb_grp.inode_ops.push(InodeOps::UpdateOrphanLink(log))
                }
                Event::InodeAllocationOnBg(InodeAllocationOnBg {
                    bgid,
                    bitmap_lba,
                    ofs,
                    de,
                }) => {
                    let bg = wb_grp.bg_delta(bgid);
                    bg.free_inodes_cnt_delta -= 1;
                    if matches!(de, FileType::Directory) {
                        bg.used_dirs_cnt_delta += 1;
                    }
                    bg.max_alloc_idx_in_bg = if let Some(pmax) = bg.max_alloc_idx_in_bg {
                        Some(core::cmp::max(pmax, ofs as u32))
                    } else {
                        Some(ofs as u32)
                    };
                    wb_grp.inode_ops.push(InodeOps::Init(
                        InodeNumber::from_bgid_index(bgid, ofs as usize, &fs.sb),
                        de,
                    ));
                    wb_grp.set_bitmap(bitmap_lba, ofs, 1, collector, fs);
                }
                Event::InodeDeallocationOnBg(InodeDeallocationOnBg {
                    ino,
                    bgid,
                    bitmap_lba,
                    ofs,
                    de,
                }) => {
                    wb_grp.bg_delta(bgid).free_inodes_cnt_delta += 1;
                    if matches!(de, FileType::Directory) {
                        wb_grp.bg_delta(bgid).used_dirs_cnt_delta -= 1;
                    }
                    wb_grp.unset_bitmap(bitmap_lba, ofs, 1, collector, fs);
                    collector
                        .postludes
                        .borrow_mut()
                        .push(PostludeOps::InodeDeallocation(ino));
                }
                Event::InodeAddLink(add) => {
                    wb_grp.inode_ops.push(InodeOps::AddLink(add));
                }
                Event::InodeRmLink(rm) => {
                    wb_grp.inode_ops.push(InodeOps::RmLink(rm));
                }
                Event::FreeInodesCountDecOnSb => wb_grp.sb_delta().free_inodes_cnt_delta -= 1,
                Event::FreeInodesCountIncOnSb => wb_grp.sb_delta().free_inodes_cnt_delta += 1,
                Event::InodeSetSize(op) => {
                    wb_grp.inode_ops.push(InodeOps::SetSize(op));
                }
            }
        }
        wb_grp
    }

    pub fn apply_on_disk<C: Config, const BLK_SIZE: usize>(
        &mut self,
        fs: &Arc<FileSystem<C, BLK_SIZE>>,
        collector: &Collector,
    ) -> Result<(), FsError> {
        let events = self.inner.take().unwrap().into_inner();
        let wbg = Self::compress(events, collector, fs);
        wbg.flush(fs, collector)
    }
}

#[derive(Default, Debug)]
pub struct BgDelta {
    free_blocks_cnt_delta: i64,
    free_inodes_cnt_delta: i64,
    used_dirs_cnt_delta: i64,
    max_alloc_idx_in_bg: Option<u32>,
}

#[derive(Default, Debug)]
pub struct SbDelta {
    free_inodes_cnt_delta: i64,
    free_blocks_cnt_delta: i64,
}

#[derive(Debug)]
pub enum InodeOps {
    AddLink(InodeAddLink),
    RmLink(InodeRmLink),
    Init(InodeNumber, FileType),
    SetSize(InodeSetSize),
    UpdateOrphanLink(InodeUpdateOrphanLink),
    SetBlock { ino: InodeNumber, cnt: i64 },
}

fn get_inode<'a, 'b, C: Config + 'a, const BLK_SIZE: usize>(
    fs: &'a FileSystem<C, BLK_SIZE>,
    ino: InodeNumber,
    collector: &'b Collector,
) -> Result<(BlockRef<'a, 'b, C, BLK_SIZE, true>, core::ops::Range<usize>), FsError>
where
    'a: 'b,
{
    let inode_size = fs.sb.inode_size;
    let (block_group, index_in_grp) = ino.into_bgid_index(&fs.sb);
    let byte_offset_in_group = index_in_grp as u64 * inode_size as u64;
    let inode_table_start = fs.get_block_group(block_group).inode_table_first_block;
    let lba = inode_table_start + (byte_offset_in_group as u64 / BLK_SIZE as u64);
    let offset_in_block = ((byte_offset_in_group % BLK_SIZE as u64) / inode_size as u64) as usize;

    fs.blocks.get_mut(lba, collector).map(|blk| {
        (
            blk,
            offset_in_block * inode_size..offset_in_block * inode_size + inode_size,
        )
    })
}

struct SbDirtyTracker<'a, C: Config, const BLK_SIZE: usize> {
    inner: TicketLockGuard<'a, superblock::Manipulator<C, BLK_SIZE>, C::S>,
    dirty: bool,
}

impl<'a, C: Config, const BLK_SIZE: usize> SbDirtyTracker<'a, C, BLK_SIZE> {
    fn new(fs: &'a Arc<FileSystem<C, BLK_SIZE>>) -> Self {
        Self {
            inner: fs.sb.manipulator.lock(),
            dirty: false,
        }
    }

    fn writeback(self, collector: &Collector) {
        let Self { dirty, .. } = self;
        if dirty {
            collector.set_sb_dirty();
        }
    }

    fn get_mut(&mut self) -> Result<&mut superblock::Manipulator<C, BLK_SIZE>, FsError> {
        self.dirty = true;
        Ok(&mut *self.inner)
    }
}

struct WritebackGroup<C: Config, const BLK_SIZE: usize> {
    bg_deltas: HashMap<BlockGroupId, BgDelta>,
    sb_delta: Option<SbDelta>,
    inode_ops: Vec<InodeOps>,
    _ty: core::marker::PhantomData<C>,
}

impl<C: Config, const BLK_SIZE: usize> WritebackGroup<C, BLK_SIZE> {
    pub fn new() -> Self {
        Self {
            bg_deltas: HashMap::new(),
            sb_delta: None,
            inode_ops: Vec::new(),
            _ty: core::marker::PhantomData,
        }
    }

    #[inline]
    fn bg_delta(&mut self, bgid: BlockGroupId) -> &mut BgDelta {
        self.bg_deltas.entry(bgid).or_insert_with(BgDelta::default)
    }
    #[inline]
    fn sb_delta(&mut self) -> &mut SbDelta {
        self.sb_delta.get_or_insert_with(SbDelta::default)
    }
    fn set_bitmap(
        &mut self,
        bitmap_lba: LogicalBlockNumber,
        ofs: usize,
        count: usize,
        collector: &Collector,
        fs: &FileSystem<C, BLK_SIZE>,
    ) {
        // XXX: In-memory state of bitmap is stored as other form, we can directly reflect disk state into fs.blocks.
        let bref = fs.blocks.get_mut(bitmap_lba, collector).unwrap();
        let mut blk = bref.write();
        let (os, oe) = (ofs, core::cmp::min((ofs + 7) & !7, ofs + count));
        let (ms, me) = (oe, (ofs + count) & !7);
        let (cs, ce) = (core::cmp::max(oe, me), ofs + count);
        for pos in os..oe {
            let (grp, dofs) = (pos >> 3, pos & 7);
            debug_assert_eq!(
                blk[grp] & (1 << dofs),
                0,
                "ofs: {:?} cnt: {:?} | {:?}~{:?}|{:?}~{:?}|{:?}~{:?}",
                ofs,
                count,
                os,
                oe,
                ms,
                me,
                cs,
                ce
            );
            blk[grp] |= 1 << dofs;
        }
        for grp in ms / 8..me / 8 {
            debug_assert_eq!(
                blk[grp], 0,
                "ofs: {:?} cnt: {:?} | {:?}~{:?}|{:?}~{:?}|{:?}~{:?}",
                ofs, count, os, oe, ms, me, cs, ce
            );
            blk[grp] = 0xff;
        }
        for pos in cs..ce {
            let (grp, dofs) = (pos >> 3, pos & 7);
            debug_assert_eq!(
                blk[grp] & (1 << dofs),
                0,
                "ofs: {:?} cnt: {:?} | {:?}~{:?}|{:?}~{:?}|{:?}~{:?}",
                ofs,
                count,
                os,
                oe,
                ms,
                me,
                cs,
                ce
            );
            blk[grp] |= 1 << dofs;
        }
    }
    fn unset_bitmap(
        &mut self,
        bitmap_lba: LogicalBlockNumber,
        ofs: usize,
        count: usize,
        collector: &Collector,
        fs: &FileSystem<C, BLK_SIZE>,
    ) {
        // XXX: In-memory state of bitmap is stored as other form, we can directly reflect disk state into fs.blocks.
        let bref = fs.blocks.get_mut(bitmap_lba, collector).unwrap();
        let mut blk = bref.write();
        let (os, oe) = (ofs, core::cmp::min((ofs + 7) & !7, ofs + count));
        let (ms, me) = (oe, (ofs + count) & !7);
        let (cs, ce) = (core::cmp::max(oe, me), ofs + count);
        for pos in os..oe {
            let (grp, ofs) = (pos >> 3, pos & 7);
            debug_assert_eq!(
                blk[grp] & (1 << ofs),
                1 << ofs,
                "ofs: {:?} cnt: {:?} | {:?}~{:?}|{:?}~{:?}|{:?}~{:?}",
                ofs,
                count,
                os,
                oe,
                ms,
                me,
                cs,
                ce
            );
            blk[grp] &= !(1 << ofs);
        }
        for grp in ms / 8..me / 8 {
            debug_assert_eq!(
                blk[grp], 0xff,
                "ofs: {:?} cnt: {:?} | {:?}~{:?}|{:?}~{:?}|{:?}~{:?}",
                ofs, count, os, oe, ms, me, cs, ce
            );
            blk[grp] = 0;
        }
        for pos in cs..ce {
            let (grp, ofs) = (pos >> 3, pos & 7);
            debug_assert_eq!(
                blk[grp] & (1 << ofs),
                1 << ofs,
                "ofs: {:?} cnt: {:?} | {:?}~{:?}|{:?}~{:?}|{:?}~{:?}",
                ofs,
                count,
                os,
                oe,
                ms,
                me,
                cs,
                ce
            );
            blk[grp] &= !(1 << ofs);
        }
    }

    pub fn flush(
        self,
        fs: &Arc<FileSystem<C, BLK_SIZE>>,
        collector: &Collector,
    ) -> Result<(), FsError> {
        // FIXME: Flush the journal into disk.
        let WritebackGroup {
            bg_deltas,
            sb_delta,
            inode_ops,
            ..
        } = self;

        // We need to resolve sb first.
        let mut raw_sb = SbDirtyTracker::new(fs);

        for (bgid, delta) in bg_deltas.into_iter() {
            Self::submit_block_group(fs, bgid, delta, collector)?;
        }
        for op in inode_ops.into_iter() {
            Self::submit_inode_ops(fs, op, &mut raw_sb, collector)?;
        }
        Self::submit_sb(sb_delta, raw_sb, collector)?;
        Ok(())
    }

    fn submit_block_group(
        fs: &FileSystem<C, BLK_SIZE>,
        bgid: BlockGroupId,
        BgDelta {
            free_blocks_cnt_delta,
            free_inodes_cnt_delta,
            used_dirs_cnt_delta,
            max_alloc_idx_in_bg: _,
        }: BgDelta,
        collector: &Collector,
    ) -> Result<(), FsError> {
        if free_blocks_cnt_delta != 0 || free_inodes_cnt_delta != 0 || used_dirs_cnt_delta != 0 {
            let (block_id, off) = bgid.into_lba_index(&fs.sb);
            let bg_arr = fs.blocks.get_mut(block_id, collector)?;
            let mut guard = bg_arr.write();
            let mut manipulator =
                block_group::Manipulator::new(&mut guard[off..off + fs.sb.block_desc_size]);
            // TODO: itable_unused.
            // if max_alloc_idx_in_bg >=
            // if idx_in_bg as u32 >= fs.sb.get_inodes_in_bg(bgid) -
            // itable_unused {     itable_unused =
            // fs.sb.get_inodes_in_bg(bgid) - idx_in_bg - 1
            // }
            // if fs.sb.features_readonly.contains(Ext4FeatureReadOnly::
            // METADATA_CSUM) { fill_csum() } if is_dir {
            // bg.inc_used_dir() };

            let cnt = manipulator.free_blocks_count().get() as i64 + free_blocks_cnt_delta;
            manipulator.free_blocks_count().set(cnt.try_into().unwrap());

            let cnt = manipulator.free_inodes_count().get() as i64 + free_inodes_cnt_delta;
            manipulator.free_inodes_count().set(cnt.try_into().unwrap());

            let cnt = manipulator.used_dirs_count().get() as i64 + used_dirs_cnt_delta;
            manipulator.used_dirs_count().set(cnt.try_into().unwrap());

            let csum = block_group::BlockGroup::calculate_csum(bgid, &mut manipulator, &fs.sb);
            manipulator.checksum().set(csum);
        }

        Ok(())
    }

    fn submit_inode_ops(
        fs: &Arc<FileSystem<C, BLK_SIZE>>,
        op: InodeOps,
        raw_sb: &mut SbDirtyTracker<C, BLK_SIZE>,
        collector: &Collector,
    ) -> Result<(), FsError> {
        match op {
            InodeOps::AddLink(InodeAddLink { ino }) => {
                let (raw, range) = get_inode(fs, ino, collector)?;
                let mut guard = raw.write();
                let mut child = inode::Manipulator::new(&mut guard[range]);
                let link_cnt = child.links_count().get() + 1;
                child.links_count().set(link_cnt);
            }
            InodeOps::RmLink(InodeRmLink { ino }) => {
                let (raw, range) = get_inode(fs, ino, collector)?;
                let mut guard = raw.write();
                let mut inode = inode::Manipulator::new(&mut guard[range]);
                let new_count = inode.links_count().get() - 1;
                if new_count == 0 {
                    let sb = raw_sb.get_mut()?;
                    inode.deletion_time().set(sb.last_orphan().get());
                    sb.last_orphan().set(ino.0);
                    fs.sb
                        .replace_orphan_prevlink(ino, InodeNumber(inode.deletion_time().get()));
                    fs.sb.add_orphan_link(
                        InodeNumber(0),
                        ino,
                        InodeNumber(inode.deletion_time().get()),
                    );
                }
                inode.links_count().set(new_count);
            }
            InodeOps::Init(ino, ftype) => {
                let (raw, range) = get_inode(fs, ino, collector)?;
                let mut guard = raw.write();
                inode::Manipulator::new(&mut guard[range]).init(fs, ftype);
            }
            InodeOps::SetSize(InodeSetSize { ino, size, address }) => {
                let (raw, range) = get_inode(fs, ino, collector)?;
                let mut guard = raw.write();
                let mut inode = inode::Manipulator::new(&mut guard[range]);
                inode.size(fs).set(size);
                inode.set_addresses(fs, address);
            }
            InodeOps::UpdateOrphanLink(InodeUpdateOrphanLink { pred, succ }) => {
                if pred.0 == 0 {
                    let sb = raw_sb.get_mut()?;
                    sb.last_orphan().set(succ.0);
                } else {
                    let (raw, range) = get_inode(fs, pred, collector)?;
                    let mut guard = raw.write();
                    inode::Manipulator::new(&mut guard[range])
                        .deletion_time()
                        .set(succ.0)
                }
            }
            InodeOps::SetBlock { ino, cnt } => {
                let (raw, range) = get_inode(fs, ino, collector)?;
                let mut guard = raw.write();
                let mut inode = inode::Manipulator::new(&mut guard[range]);
                let mut blk = inode.blocks(fs);
                blk.set(
                    blk.get()
                        .overflowing_add((BLK_SIZE / 512).overflowing_mul(cnt as usize).0 as u64)
                        .0,
                );
            }
        }
        Ok(())
    }

    fn submit_sb(
        sb_delta: Option<SbDelta>,
        mut raw_sb: SbDirtyTracker<C, BLK_SIZE>,
        collector: &Collector,
    ) -> Result<(), FsError> {
        if let Some(SbDelta {
            free_inodes_cnt_delta,
            free_blocks_cnt_delta,
        }) = sb_delta
        {
            let sb_manipulator = raw_sb.get_mut()?;

            let cnt = sb_manipulator.free_inodes_count().get() as i64 + free_inodes_cnt_delta;
            sb_manipulator
                .free_inodes_count()
                .set(cnt.try_into().unwrap());

            let cnt = sb_manipulator.free_blocks_count().get() as i64 + free_blocks_cnt_delta;
            sb_manipulator
                .free_blocks_count()
                .set(cnt.try_into().unwrap());
        }
        raw_sb.writeback(collector);
        Ok(())
    }
}
