use crate::types::Zero;
use crate::utils::ByteRw;
use crate::{Config, FsError};

#[repr(transparent)]
pub(crate) struct Wrapper<C: Config>(pub C::Buffer<1024>);

impl<C: Config> core::convert::AsRef<[u8]> for Wrapper<C> {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref().as_ref()
    }
}

impl<C: Config> core::convert::AsMut<[u8]> for Wrapper<C> {
    fn as_mut(&mut self) -> &mut [u8] {
        self.0.as_mut().as_mut()
    }
}

pub(crate) struct Manipulator<C: Config, const N: usize> {
    pub rw: ByteRw<Wrapper<C>>,
}

impl<C: Config, const N: usize> Manipulator<C, N> {
    #[inline]
    pub fn from_disk(dev: &C) -> Result<Self, FsError> {
        let b = if N == 1024 {
            dev.read_bytes::<1024>(N)?
        } else {
            let mut b = C::Buffer::<1024>::zeroed();
            let o = dev.read_bytes::<N>(0)?;
            b.as_mut().copy_from_slice(&o[1024..2048]);
            b
        };

        Ok(Self {
            rw: ByteRw::new(Wrapper(b)),
        })
    }

    #[inline]
    pub fn writeback(&self, dev: &C) -> Result<(), FsError> {
        // FIXME
        dev.write_bytes(1024, &self.rw.inner().0).map(|_| ())
    }
}

impl<C: Config, const N: usize> Manipulator<C, N> {
    pub fn from_bytes(b: C::Buffer<1024>) -> Self {
        Self {
            rw: ByteRw::new(Wrapper(b)),
        }
    }

    pub fn is_feature_incompat64(&self) -> bool {
        use super::Ext4FeatureIncompatible;

        Ext4FeatureIncompatible::from_bits_truncate(self.rw.read_u32(0x60))
            .contains(Ext4FeatureIncompatible::BIT64)
    }

    crate::fs_field! {
        ty: Wrapper<C>;
        /// Total inode count.
        inodes_count : @0x0, u32;
        /// Total block count.
        blocks_count : (@0x4, @0x150 if Self::is_feature_incompat64), u64;
        /// This number of blocks can only be allocated by the super-user.
        r_blocks_count: (@0x8, @0x154 if Self::is_feature_incompat64), u64;
        /// Free block count.
        free_blocks_count : (@0xC, @0x158 if Self::is_feature_incompat64), u64;
        /// Free inode count.
        free_inodes_count : @0x10, u32;
        /// First data block. This must be at least 1 for 1k-block filesystems and is typically 0 for all other block sizes.
        first_data_block : @0x14, u32;
        /// Block size is 2 ^ (10 + s_log_block_size).
        log_block_size : @0x18, u32;
        /// Cluster size is (2 ^ s_log_cluster_size) blocks if bigalloc is enabled. Otherwise s_log_cluster_size must equal s_log_block_size.
        log_cluster_size : @0x1C, u32;
        /// Blocks per group.
        blocks_per_group : @0x20, u32;
        /// Clusters per group, if bigalloc is enabled. Otherwise s_clusters_per_group must equal s_blocks_per_group.
        clusters_per_group : @0x24, u32;
        /// Inodes per group.
        inodes_per_group : @0x28, u32;
        /// Mount time, in seconds since the epoch.
        mtime: @0x2C, u32;
        /// Write time, in seconds since the epoch.
        wtime: @0x30, u32;
        /// Number of mounts since the last fsck.
        mount_count: @0x34, u16;
        /// Number of mounts beyond which a fsck is needed.
        max_mount_count : @0x36, u32;
        /// Magic signature, 0xEF53
        magic : @0x38, u16;
        /// File system state.
        state : @0x3A, u16;
        /// Behaviour when detecting errors.
        errors : @0x3C, u16;
        /// Minor revision level.
        minor_rev_level : @0x3E, u16;
        /// Time of last check, in seconds since the epoch.
        lastcheck: @0x40, u32;
        /// Maximum time between checks, in seconds.
        check_interval: @0x44, u32;
        /// OS.
        creator_os : @0x48, u32;
        /// Revision level.
        rev_level : @0x4C, u32;
        /// Default uid for reserved blocks.
        def_resuid: @0x50, u16;
        /// Default gid for reserved blocks.
        def_resgid: @0x52, u16;
        /// First non-reserved inode.
        first_inode : @0x54, u32;
        /// Size of inode structure, in bytes.
        inode_size: @0x58, u16;
        /// Block group # of this superblock.
        block_group_nr: @0x5A, u16;
        /// Compatible feature set flags.
        feature_compat: @0x5C, u32;
        /// Incompatible feature set.
        feature_incompat: @0x60, u32;
        /// Readonly-compatible feature set.
        feature_ro_compat: @0x64, u32;
        /// Number of reserved GDT entries for future filesystem expansion.
        reserverd_gdt_blocks: @0xCE, u16;
        /// Start of list of orphaned inodes to delete.
        last_orphan: @0xE8, u32;
        /// Default hash algorithm to use for directory hashes.
        default_hash_version: @0xFC, u8;
        /// Size of group descriptors, in bytes, if the 64bit incompat feature flag is set.
        desc_size: @0xFE, u16;
        /// Default mount options.
        default_mount_opts : @0x100, u32;
        /// First metablock block group, if the meta_bg feature is enabled.
        first_meta_bg: @0x104, u32;
        /// When the filesystem was created, in seconds since the epoch.
        mkfs_time: @0x108, u32;
        /// All inodes have at least # bytes.
        min_extra_isize: @0x15C, u16;
        /// New inodes should reserve # bytes.
        want_extra_isize: @0x15E, u16;
        /// Miscellaneous flags.
        flags: @0x160, u32;
        /// Metadata checksum algorithm type. The only valid value is 1 (crc32c).
        checksum_type: @0x175, u8;
    }
}
