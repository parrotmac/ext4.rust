use super::{Ext4De, InodeAddressingMode, InodeFlag, RawInodeAddressingMode};
use crate::filesystem::FileSystem;
use crate::superblock::{Ext4FeatureIncompatible, Ext4FeatureReadOnly};
use crate::utils::{ByteRw, CompoundAccessorU32, CompoundAccessorU64};
use crate::{Config, FileType, InodeNumber};
use core::convert::TryInto;

const EXT4_GOOD_OLD_INODE_SIZE: usize = 0x80;

pub(crate) struct Manipulator<T>
where
    T: core::convert::AsRef<[u8]>,
{
    rw: ByteRw<T>,
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
    pub(crate) fn mode<C: Config, const BLK_SIZE: usize>(
        &mut self,
        fs: &FileSystem<C, BLK_SIZE>,
    ) -> CompoundAccessorU32<T, 0x0, 0x76> {
        const EXT4_SUPERBLOCK_OS_HURD: u32 = 1;
        CompoundAccessorU32::new(&mut self.rw, fs.sb.creator_os == EXT4_SUPERBLOCK_OS_HURD)
    }

    #[inline]
    pub(crate) fn size<C: Config, const BLK_SIZE: usize>(
        &mut self,
        fs: &FileSystem<C, BLK_SIZE>,
    ) -> CompoundAccessorU64<T, 0x4, 0x6C> {
        let use_hi = fs.sb.rev_level > 0 && matches!(self.detect_type(), FileType::RegularFile);
        CompoundAccessorU64::new(&mut self.rw, use_hi)
    }

    #[inline]
    pub(crate) fn blocks<C: Config, const BLK_SIZE: usize>(
        &mut self,
        fs: &FileSystem<C, BLK_SIZE>,
    ) -> CompoundAccessorU64<T, 0x1C, 0x74> {
        let use_hi = fs
            .sb
            .features_readonly
            .contains(Ext4FeatureReadOnly::HUGE_FILE);
        CompoundAccessorU64::new(&mut self.rw, use_hi)
    }

    crate::fs_field! {
        ty: T;
        uid: @0x2, u16;
        // size
        access_time: @0x8, u32;
        change_time: @0xC, u32;
        modification_time: @0x10, u32;
        deletion_time: @0x14, u32;
        gid: @0x18, u16;
        links_count: @0x1A, u16;
        // blocks.
        flags: @0x20, u32;
        // osd1.
        // iblocks.
        generation: @0x64, u32;
        file_acl: @0x68, u32;
        obsolete_fragment_addr: @0x80, u32;
        // osd2.
        extra_isize: @0x80, u16;
        creation_time: @0x90, u32;
    }

    #[inline]
    pub(crate) fn get_addressing_mode<C: Config, const BLK_SIZE: usize>(
        &mut self,
        fs: &FileSystem<C, BLK_SIZE>,
    ) -> InodeAddressingMode<C> {
        if fs
            .sb
            .features_incompatible
            .contains(Ext4FeatureIncompatible::EXTENTS)
            && InodeFlag::from_bits_truncate(self.flags().get()).contains(InodeFlag::EXTENTS)
        {
            InodeAddressingMode::Extent(super::ExtentTree {
                entries_cnt: self.rw.read_u16(0x2a),
                max_entries_cnt: self.rw.read_u16(0x2c),
                depth: self.rw.read_u16(0x2e),
                b: self.rw.inner().as_ref()[0x34..0x64].try_into().unwrap(),
                ..Default::default()
            })
        } else {
            InodeAddressingMode::Legacy(super::Legacy {
                addresses: [
                    self.rw.read_u32(0x28),
                    self.rw.read_u32(0x2c),
                    self.rw.read_u32(0x30),
                    self.rw.read_u32(0x34),
                    self.rw.read_u32(0x38),
                    self.rw.read_u32(0x3c),
                    self.rw.read_u32(0x40),
                    self.rw.read_u32(0x44),
                    self.rw.read_u32(0x48),
                    self.rw.read_u32(0x4c),
                    self.rw.read_u32(0x50),
                    self.rw.read_u32(0x54),
                    self.rw.read_u32(0x58),
                    self.rw.read_u32(0x5c),
                    self.rw.read_u32(0x60),
                ],
            })
        }
    }

    #[inline]
    pub(super) fn checksum(&mut self) -> CompoundAccessorU32<T, 0x7c, 0x82> {
        let use_hi = self.rw.inner().as_ref().len() > EXT4_GOOD_OLD_INODE_SIZE;
        CompoundAccessorU32::new(&mut self.rw, use_hi)
    }

    pub(crate) fn calculate_checksum<C: Config, const BLK_SIZE: usize>(
        &mut self,
        fs: &FileSystem<C, BLK_SIZE>,
        ino: InodeNumber,
    ) -> Option<u32> {
        if fs
            .sb
            .features_readonly
            .contains(Ext4FeatureReadOnly::METADATA_CSUM)
        {
            use crate::crc::Crc32c;

            let mut hasher = Crc32c::default();
            hasher.write(&fs.sb.uuid);
            hasher.write(&ino.0.to_ne_bytes());
            hasher.write(&self.generation().get().to_ne_bytes());

            let b = self.rw.inner().as_ref();
            // start .. i_checksum_lo
            hasher.write(&b[..0x7c]);
            // i_checksum_lo
            hasher.write(&0_u16.to_ne_bytes());
            // i_checksum_lo + 2 .. i_checksum_hi
            hasher.write(&b[0x7e..0x82]);
            // i_checksum_hi
            hasher.write(&0_u16.to_ne_bytes());
            // remainder
            hasher.write(&b[0x84..]);
            Some(hasher.finish())
        } else {
            None
        }
    }

    #[inline]
    pub fn detect_type(&self) -> FileType {
        match self.rw.read_u16(0) & 0xF000 {
            0x1000 => FileType::Fifo,
            0x2000 => FileType::CharacterDev,
            0x4000 => FileType::Directory,
            0x6000 => FileType::BlockDev,
            0x8000 => FileType::RegularFile,
            0xA000 => FileType::Symlink,
            0xC000 => FileType::Socket,
            _ => FileType::Unknown,
        }
    }
}

impl<T> Manipulator<T>
where
    T: core::convert::AsRef<[u8]>,
    T: core::convert::AsMut<[u8]>,
{
    #[inline]
    fn filetype_to_mode(ft: FileType) -> u32 {
        match ft {
            FileType::Fifo => 0x1000,
            FileType::CharacterDev => 0x2000,
            FileType::Directory => 0x4000,
            FileType::BlockDev => 0x6000,
            FileType::RegularFile => 0x8000,
            FileType::Symlink => 0xA000,
            FileType::Socket => 0xC000,
            _ => unreachable!(),
        }
    }

    #[inline]
    pub(crate) fn set_addresses<C: Config, const BLK_SIZE: usize>(
        &mut self,
        fs: &FileSystem<C, BLK_SIZE>,
        mode: RawInodeAddressingMode,
    ) {
        let has_extent =
            InodeFlag::from_bits_truncate(self.flags().get()).contains(InodeFlag::EXTENTS);
        assert!(
            fs.sb
                .features_incompatible
                .contains(Ext4FeatureIncompatible::EXTENTS)
                || !has_extent
        );
        match (mode, has_extent) {
            (
                RawInodeAddressingMode::Extent {
                    entries_cnt,
                    max_entries_cnt,
                    depth,
                    b,
                },
                true,
            ) => {
                self.rw.write_u16(0x28, 0xF30A);
                self.rw.write_u16(0x2a, entries_cnt);
                self.rw.write_u16(0x2c, max_entries_cnt);
                self.rw.write_u16(0x2e, depth);
                self.rw.write_u16(0x30, 0);
                self.rw.inner_mut().as_mut()[0x34..0x64].copy_from_slice(&b)
            }
            (RawInodeAddressingMode::Legacy(le), false) => {
                self.rw.write_u32(0x28, le.addresses[0]);
                self.rw.write_u32(0x2c, le.addresses[1]);
                self.rw.write_u32(0x30, le.addresses[2]);
                self.rw.write_u32(0x34, le.addresses[3]);
                self.rw.write_u32(0x38, le.addresses[4]);
                self.rw.write_u32(0x3c, le.addresses[5]);
                self.rw.write_u32(0x40, le.addresses[6]);
                self.rw.write_u32(0x44, le.addresses[7]);
                self.rw.write_u32(0x48, le.addresses[8]);
                self.rw.write_u32(0x4c, le.addresses[9]);
                self.rw.write_u32(0x50, le.addresses[10]);
                self.rw.write_u32(0x54, le.addresses[11]);
                self.rw.write_u32(0x58, le.addresses[12]);
                self.rw.write_u32(0x5c, le.addresses[13]);
                self.rw.write_u32(0x60, le.addresses[14]);
            }
            _ => unreachable!("{:?}", InodeFlag::from_bits_truncate(self.flags().get())),
        }
    }

    #[inline]
    pub fn init<C: Config, const BLK_SIZE: usize>(
        &mut self,
        fs: &FileSystem<C, BLK_SIZE>,
        ftype: FileType,
    ) {
        let mut flag = InodeFlag::empty();

        let addresses = if fs
            .sb
            .features_incompatible
            .contains(Ext4FeatureIncompatible::EXTENTS)
            && matches!(ftype, FileType::Directory | FileType::RegularFile)
        {
            flag |= InodeFlag::EXTENTS;
            RawInodeAddressingMode::Extent {
                entries_cnt: 0,
                max_entries_cnt: 4,
                depth: 0,
                b: [0; 48],
            }
        } else {
            RawInodeAddressingMode::Legacy(crate::inode::legacy::Legacy { addresses: [0; 15] })
        };

        if matches!(ftype, FileType::Directory) {
            flag |= InodeFlag::INDEX;
        }

        self.rw.inner_mut().as_mut().fill(0);
        self.mode(fs).set(
            Ext4De::from_file_type(ftype).default_mode().bits() | Self::filetype_to_mode(ftype),
        );
        self.flags().set(flag.bits());
        self.set_addresses(fs, addresses);
        if fs.sb.inode_size > EXT4_GOOD_OLD_INODE_SIZE {
            self.extra_isize().set(fs.sb.want_extra_isize)
        }
    }
}
