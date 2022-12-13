use crate::block_group::{self, BlockGroup, BlockGroupFlag};
use crate::inode::Inode;
use crate::superblock::{
    self, Ext4FeatureCompatible, Ext4FeatureIncompatible, Ext4FeatureReadOnly, SuperBlock,
};
use crate::types::Zero;
use crate::utils::ByteRw;
use crate::{BlockGroupId, Config, FileSystem, FileType, FsError, LogicalBlockNumber};
use alloc::collections::BTreeMap;
use alloc::sync::Arc;
const DESC_SIZE: u16 = 32;

#[derive(Clone, Copy)]
struct FormatAux<'a> {
    blocks_cnt: u64,
    inodes_cnt: u32,
    feat_ro: Ext4FeatureReadOnly,
    feat_com: Ext4FeatureCompatible,
    feat_incom: Ext4FeatureIncompatible,
    uuid: &'a [u8; 16],
    volumn_name: &'a [u8; 16],
    hash_seed: &'a [u8; 16],
    blocks_per_group: u32,
    first_data_block: u32,
    block_size: usize,
    inodes_per_group: u32,
}

impl<'a> FormatAux<'a> {
    fn new(
        total_size: usize,
        block_size: usize,
        uuid: &'a [u8; 16],
        volumn_name: &'a [u8; 16],
        hash_seed: &'a [u8; 16],
    ) -> Self {
        let blocks_cnt = (total_size / (block_size as usize)) as u64;
        let blocks_per_group = (block_size as u32) * 8;
        let block_group = (blocks_cnt + (blocks_per_group as u64) - 1) / (blocks_per_group as u64);
        let inodes_per_group = {
            // round up to 8.
            let tmp: u32 = (blocks_cnt / block_group / 4).try_into().unwrap();
            (tmp + 7) / 8 * 8
        };
        let inodes_cnt = inodes_per_group.checked_mul(block_group as u32).unwrap();

        Self {
            blocks_cnt,
            inodes_cnt,
            block_size,
            feat_ro: Ext4FeatureReadOnly::SPARSE_SUPER
                | Ext4FeatureReadOnly::LARGE_FILE
                | Ext4FeatureReadOnly::GDT_CSUM,
            feat_com: Ext4FeatureCompatible::DIR_INDEX,
            feat_incom: Ext4FeatureIncompatible::FILETYPE | Ext4FeatureIncompatible::EXTENTS,
            uuid,
            volumn_name,
            hash_seed,
            blocks_per_group,
            first_data_block: if block_size == 1024 { 1 } else { 0 },
            inodes_per_group,
        }
    }
}

fn make_sb<C: Config, const BLK_SIZE: usize>(
    FormatAux {
        blocks_cnt,
        block_size,
        inodes_cnt,
        feat_com,
        feat_incom,
        feat_ro,
        uuid,
        blocks_per_group,
        volumn_name,
        hash_seed,
        first_data_block,
        inodes_per_group,
    }: FormatAux,
) -> SuperBlock<C, BLK_SIZE> {
    let mut sb = superblock::Manipulator::from_bytes(C::Buffer::<1024>::zeroed());
    sb.feature_compat().set(feat_com.bits());
    sb.feature_incompat().set(feat_incom.bits());
    sb.feature_ro_compat().set(feat_ro.bits());
    sb.inodes_count().set(inodes_cnt);
    sb.free_inodes_count().set(inodes_cnt - 9);
    sb.blocks_count().set(blocks_cnt);
    sb.free_blocks_count().set(0);
    sb.first_data_block().set(first_data_block);
    sb.log_block_size().set(block_size.trailing_zeros() - 10);
    sb.log_cluster_size().set(block_size.trailing_zeros() - 10);
    sb.blocks_per_group().set(blocks_per_group);
    sb.clusters_per_group().set(blocks_per_group);
    sb.inodes_per_group().set(inodes_per_group);
    sb.max_mount_count().set(0xffff);
    sb.magic().set(0xEF53);
    sb.state().set(1);
    sb.errors().set(2);
    sb.minor_rev_level().set(0);
    sb.creator_os().set(0);
    sb.rev_level().set(1);
    sb.first_inode().set(11);
    sb.inode_size().set(256);

    sb.rw.b.as_mut()[0x68..0x78].copy_from_slice(uuid);
    sb.rw.b.as_mut()[0x78..0x88].copy_from_slice(volumn_name);
    sb.rw.b.as_mut()[0xEC..0xFC].copy_from_slice(hash_seed);

    sb.default_hash_version()
        .set(crate::hasher::HashVersion::HalfMd4 as u8);
    sb.checksum_type().set(1);
    sb.desc_size().set(DESC_SIZE);
    sb.flags().set(1);
    sb.min_extra_isize().set(0x20);
    sb.want_extra_isize().set(0x20);
    SuperBlock::from_raw(sb).unwrap()
}

fn fill_bg<C: Config, const BLK_SIZE: usize>(
    dev: &C,
    sb: &mut SuperBlock<C, BLK_SIZE>,
) -> Result<(), FsError> {
    let bgs_count =
        ((sb.blocks_count - (sb.first_data_block as u64) + (sb.blocks_per_group as u64) - 1)
            / (sb.blocks_per_group as u64)) as u32;
    let desc_blocks = ((sb.block_desc_size * bgs_count as usize + BLK_SIZE - 1) / BLK_SIZE) as u64;
    let inode_blocks = (((sb.inodes_per_group as usize) * (sb.inode_size as usize) + BLK_SIZE - 1)
        / BLK_SIZE) as u32;

    let mut sb_manipulator = sb.manipulator.lock();
    let mut block_map = BTreeMap::new();
    let mut total_blocks = sb.blocks_count;

    for bgid in (0..bgs_count as u32).map(BlockGroupId) {
        let (lba, index) = bgid.into_lba_index(sb);
        let block_base = (bgid.0 as u64) * (sb.blocks_per_group as u64);
        let mut start_block = (sb.first_data_block as u64) + block_base + desc_blocks;

        // Cacluate free blocks and free inodes in group.
        let free_blocks = core::cmp::min(sb.blocks_per_group as u64, total_blocks) as u32
            - inode_blocks
            - if bgid.0 == bgs_count - 1 {
                2 + sb.first_data_block
            } else {
                2
            }
            - if sb.is_super_in_bg(bgid) {
                start_block += 1;
                1 + desc_blocks as u32
            } else {
                0
            };
        let mut free_inodes = sb.inodes_per_group;

        let block_bitmap_pad_back =
            core::cmp::min(total_blocks as usize, sb.blocks_per_group as usize);

        total_blocks = total_blocks.saturating_sub(sb.blocks_per_group as u64);
        // if bgid is zero, init inode table and bitmap.
        // Otherwise, initialize lazily.
        if bgid.0 == 0 {
            // Fill inode bitmap
            let mut inode_bitmap = ByteRw::new(
                block_map
                    .entry(LogicalBlockNumber(start_block + 2))
                    .or_insert_with(|| C::Buffer::<BLK_SIZE>::zeroed())
                    .as_mut(),
            );
            inode_bitmap.set_bitmap(0..1);
            inode_bitmap.set_bitmap(2..10);
            free_inodes -= 9;
            // Set end of inode bitmap. kill 1) unusable 2) padding.
            inode_bitmap.set_bitmap(sb.inodes_per_group as usize..BLK_SIZE * 8);

            // zero out the inode table
            for lba in
                (start_block + 3..start_block + 3 + inode_blocks as u64).map(LogicalBlockNumber)
            {
                block_map
                    .entry(lba)
                    .or_insert_with(|| C::Buffer::<BLK_SIZE>::zeroed());
            }

            // Fill block bitmap
            let mut block_bitmap = ByteRw::new(
                block_map
                    .entry(LogicalBlockNumber(start_block + 1))
                    .or_insert_with(|| C::Buffer::<BLK_SIZE>::zeroed())
                    .as_mut(),
            );
            if sb.is_super_in_bg(bgid) {
                block_bitmap.set_bitmap(0..1 + sb.first_data_block as usize + desc_blocks as usize);
            }
            // Set block bitmap, inode bitmap, inode table as used block.
            block_bitmap.set_bitmap(
                (start_block - block_base) as usize + 1
                    ..=(start_block - block_base) as usize + 2 + inode_blocks as usize,
            );
            // Set end of block bitmap. kill 1) unusable 2) padding.
            block_bitmap.set_bitmap(block_bitmap_pad_back..BLK_SIZE * 8);
        }

        // Fill backup sb
        if sb.is_super_in_bg(bgid) && bgid.0 != 0 {
            let ofs = BLK_SIZE
                * (sb.first_data_block as usize + bgid.0 as usize * sb.blocks_per_group as usize);
            dev.write_bytes(ofs, &sb_manipulator.rw.inner().0)?;
        }

        // Fill bg meta
        {
            let mut bg = block_group::Manipulator::new(
                &mut block_map
                    .entry(lba)
                    .or_insert_with(|| C::Buffer::<BLK_SIZE>::zeroed())
                    .as_mut()[index..index + sb.block_desc_size],
            );

            bg.block_bitmap().set(start_block + 1);
            bg.inode_bitmap().set(start_block + 2);
            bg.inode_table().set(start_block + 3);
            bg.free_blocks_count().set(free_blocks);
            bg.free_inodes_count().set(free_inodes);
            bg.used_dirs_count().set(0);
            if bgid.0 != 0 {
                bg.flags()
                    .set((BlockGroupFlag::INODE_UNINIT | BlockGroupFlag::BLOCK_UNINIT).bits());
            }

            let csum = BlockGroup::calculate_csum(bgid, &mut bg, sb);
            bg.checksum().set(csum);
        }

        let blocks_cnt = sb_manipulator.free_blocks_count().get();
        sb_manipulator
            .free_blocks_count()
            .set(blocks_cnt + free_blocks as u64);
    }
    for (lba, block) in block_map.into_iter() {
        dev.write_bytes(lba.0 as usize * BLK_SIZE, &block)?;
    }
    Ok(())
}

fn make_root<C: Config, const BLK_SIZE: usize>(
    fs: &Arc<FileSystem<C, BLK_SIZE>>,
) -> Result<(), FsError> {
    let ino = fs.inodes.allocator.try_allocate_at(2, fs).unwrap();
    let tx = fs.open_transaction();
    let bg = fs.get_block_group(BlockGroupId(0));
    let de = FileType::Directory;

    bg.allocate_inode_on_bg(1, &tx, de);
    fs.sb.dec_free_inodes_count(&tx);
    let inode = fs
        .inodes
        .inodes
        .get_or_insert::<_, ()>(ino, |ino| Ok(Inode::new(fs, ino, de)))
        .unwrap();
    let root = inode.into_fs_object().get_directory().unwrap();
    crate::dispatch_scheme!(root.get_scheme(fs), |s| {
        s.add_entry(fs, ".", FileType::Directory, ino, &tx)?;
        s.add_entry(fs, "..", FileType::Directory, ino, &tx)?;
        root.inode.rw.write().links_count += 1;
        tx.inode_add_link(ino);
        tx.inode_add_link(ino);
    });
    tx.done(fs)?;

    let tx = fs.open_transaction();
    root.create_entry("lost+found", FileType::Directory, &tx)?;
    tx.done(fs)
}

/// Do a filesystem format.
pub fn format<C: Config, const BLK_SIZE: usize>(
    dev: C,
    uuid: &[u8; 16],
    volumn_name: &[u8; 16],
    hash_seed: &[u8; 16],
) -> Result<Arc<FileSystem<C, BLK_SIZE>>, FsError> {
    //      +----- block groups --------+------------------ Bg0 ----------------------+--------------------- Bg1 --------------------+
    //      v                           v                                             v                                              v
    // | SB | BG0 BG1 .. | .. BGn [pad] | rgdt .. | block_bitmap | inode_bitmap | ... | rgdt .. | block_bitmap | inode_bitmap | ... | ...
    let aux = FormatAux::new(
        dev.total_size() & !(BLK_SIZE - 1),
        BLK_SIZE,
        uuid,
        volumn_name,
        hash_seed,
    );
    let mut sb = make_sb::<C, BLK_SIZE>(aux);
    fill_bg(&dev, &mut sb)?;
    dev.write_bytes(1024, &sb.manipulator.lock().rw.inner().0)?;
    let fs = FileSystem::<C, BLK_SIZE>::new(dev, sb)?;
    make_root(&fs)?;
    Ok(fs)
}

#[cfg(test)]
mod tests {
    use crate::tests::{make_disk, test_oracle};

    const M: u64 = 1024 * 1024;
    const G: u64 = 1024 * 1024 * 1024;

    fn run(path: std::path::PathBuf) -> bool {
        let r = test_oracle(path.as_path());
        let _ = std::fs::remove_file(path);
        println!("next");
        r
    }

    #[test]
    fn format_small() {
        assert!(run(make_disk(4096, 100 * M)));
        assert!(run(make_disk(4096, 200 * M)));
        assert!(run(make_disk(4096, 300 * M)));
        assert!(run(make_disk(4096, 400 * M)));
        assert!(run(make_disk(4096, 500 * M)));
        assert!(run(make_disk(4096, 600 * M)));
        assert!(run(make_disk(4096, 700 * M)));
        assert!(run(make_disk(4096, 800 * M)));
        assert!(run(make_disk(4096, 900 * M)));
        assert!(run(make_disk(4096, 1000 * M)));
    }
    #[test]
    fn format_big_10g() {
        assert!(run(make_disk(4096, 10 * G)));
    }
    #[test]
    fn format_unaligned() {
        assert!(run(make_disk(4096, 100 * M - 1234)));
        assert!(run(make_disk(4096, 200 * M - 5678)));
    }
}
