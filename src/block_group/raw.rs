use crate::utils::ByteRw;

pub(crate) struct Manipulator<T>
where
    T: core::convert::AsRef<[u8]>,
{
    pub rw: ByteRw<T>,
}

impl<T> Manipulator<T>
where
    T: core::convert::AsRef<[u8]>,
{
    #[inline]
    pub(crate) fn new(t: T) -> Self {
        Self { rw: ByteRw::new(t) }
    }

    #[inline]
    fn is_bg_64(&self) -> bool {
        self.rw.inner().as_ref().len() >= 64
    }

    crate::fs_field! {
        ty: T;
        /// location of block bitmap.
        block_bitmap : (@0x0, @0x20 if Self::is_bg_64), u64;
        /// location of inode bitmap.
        inode_bitmap : (@0x4, @0x24 if Self::is_bg_64), u64;
        /// location of inode table.
        inode_table : (@0x8, @0x28 if Self::is_bg_64), u64;
        /// free block count.
        free_blocks_count : (@0xC, @0x2C if Self::is_bg_64), u32;
        /// free inode count.
        free_inodes_count : (@0xE, @0x2E if Self::is_bg_64), u32;
        /// directory count.
        used_dirs_count : (@0x10, @0x30 if Self::is_bg_64), u32;
        /// block group flags.
        flags : @0x12, u16;
        /// location of snapshot exclusion bitmap.
        exclude_bitmap : (@0x14, @0x34 if Self::is_bg_64), u64;
        /// block bitmap checksum.
        block_bitmap_checksum : (@0x18, @0x38 if Self::is_bg_64), u32;
        /// inode bitmap checksum.
        inode_bitmap_checksum : (@0x1A, @0x3A if Self::is_bg_64), u32;
        /// unused inode count.
        ///
        /// If set, we needn't scan past the (sb.s_inodes_per_group - gdt.bg_itable_unused)th entry in the inode table for this group.
        itable_unused : (@0x1C, @0x32 if Self::is_bg_64), u32;
        /// Group descriptor checksum.
        ///
        /// crc16(sb_uuid+group_desc) if the RO_COMPAT_GDT_CSUM feature is set.
        /// crc32c(sb_uuid+group_desc) & 0xFFFF if the RO_COMPAT_METADATA_CSUM feature is set.
        checksum : @0x1E, u16;
    }
}
