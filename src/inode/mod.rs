mod allocator;
mod extent;
mod legacy;
mod manager;
mod raw;

use crate::directory::Directory;
use crate::file::File;
use crate::filesystem::FileSystem;
use crate::superblock::Ext4FeatureIncompatible;
use crate::transaction::Transaction;
use crate::{
    Config, FileBlockNumber, FileType, FsError, FsObject, InodeMode, InodeNumber,
    LogicalBlockNumber, RwLock,
};
use alloc::sync::{Arc, Weak};
use bitflags::bitflags;
use extent::ExtentTree;
use legacy::Legacy;

pub use manager::Manager;
pub(crate) use raw::Manipulator;

#[derive(Debug, Clone, Copy, Eq, PartialEq, num_enum::TryFromPrimitive)]
#[repr(u8)]
pub enum Ext4De {
    Unknown = 0,
    RegularFile = 1,
    Directory = 2,
    CharacterDev = 3,
    BlockDev = 4,
    Fifo = 5,
    Socket = 6,
    Symlink = 7,
    CheckSum = 0xDE,
}

impl Ext4De {
    pub fn from_file_type(ft: FileType) -> Self {
        match ft {
            FileType::RegularFile => Self::RegularFile,
            FileType::Directory => Self::Directory,
            FileType::CharacterDev => Self::CharacterDev,
            FileType::BlockDev => Self::BlockDev,
            FileType::Fifo => Self::Fifo,
            FileType::Socket => Self::Socket,
            FileType::Symlink => Self::Symlink,
            FileType::Unknown => Self::Unknown,
        }
    }

    pub fn into_file_type(self) -> FileType {
        match self {
            Self::RegularFile => FileType::RegularFile,
            Self::Directory => FileType::Directory,
            Self::CharacterDev => FileType::CharacterDev,
            Self::BlockDev => FileType::BlockDev,
            Self::Fifo => FileType::Fifo,
            Self::Socket => FileType::Socket,
            Self::Symlink => FileType::Symlink,
            _ => FileType::Unknown,
        }
    }

    pub fn default_mode(&self) -> InodeMode {
        match self {
            Ext4De::Fifo
            | Ext4De::CharacterDev
            | Ext4De::BlockDev
            | Ext4De::RegularFile
            | Ext4De::Socket => {
                InodeMode::WOTH
                    | InodeMode::ROTH
                    | InodeMode::WGRP
                    | InodeMode::RGRP
                    | InodeMode::WUSR
                    | InodeMode::RUSR
            }
            Ext4De::Directory | Ext4De::Symlink => {
                InodeMode::XOTH
                    | InodeMode::WOTH
                    | InodeMode::ROTH
                    | InodeMode::XGRP
                    | InodeMode::WGRP
                    | InodeMode::RGRP
                    | InodeMode::XUSR
                    | InodeMode::WUSR
                    | InodeMode::RUSR
            }
            _ => unreachable!(),
        }
    }
}

bitflags! {
    pub struct InodeFlag: u32 {
        /// Secure deletion
        const SECRM = 0x00000001;
        /// Undelete
        const UNRM = 0x00000002;
        /// Compress file
        const COMPR = 0x00000004;
        /// Synchronous updates
        const SYNC = 0x00000008;
        /// Immutable file
        const IMMUTABLE = 0x00000010;
        /// writes to file may only append
        const APPEND = 0x00000020;
        /// do not dump file
        const NODUMP = 0x00000040;
        /// do not update atime
        const NOATIME = 0x00000080;
        const DIRTY = 0x00000100;
        /// One or more compressed clusters
        const COMPRBLK = 0x00000200;
        /// Don't compress
        const NOCOMPR = 0x00000400;
        /// Compression error
        const ECOMPR = 0x00000800;
        /// hash-indexed directory
        const INDEX = 0x00001000;
        /// AFS directory
        const IMAGIC = 0x00002000;
        /// File data should be collectored
        const JOURNAL_DATA = 0x00004000;
        /// File tail should not be merged
        const NOTAIL = 0x00008000;
        /// Dirsync behaviour (directories only)
        const DIRSYNC = 0x00010000;
        /// Top of directory hierarchies
        const TOPDIR = 0x00020000;
        /// Set to each huge file
        const HUGE_FILE = 0x00040000;
        /// Inode uses extents
        const EXTENTS = 0x00080000;
        /// Inode used for large EA
        const EA_INODE = 0x00200000;
        /// Blocks allocated beyond EOF
        const EOFBLOCKS = 0x00400000;
        /// reserved for ext4 lib
        const RESERVED = 0x80000000;
    }
}

#[derive(Debug)]
pub enum AddressingOutput {
    Uninitialized,
    Initialized(LogicalBlockNumber),
}

impl AddressingOutput {
    #[inline]
    pub fn get_initialized(self) -> Option<LogicalBlockNumber> {
        if let Self::Initialized(lba) = self {
            Some(lba)
        } else {
            None
        }
    }
}

pub enum InodeAddressingMode<C: Config> {
    Legacy(Legacy),
    Extent(ExtentTree<C>),
}

impl<C: Config> core::fmt::Debug for InodeAddressingMode<C> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Legacy(l) => write!(f, "{:?}", l),
            Self::Extent(e) => write!(f, "{:?}", e),
        }
    }
}

impl<C: Config> InodeAddressingMode<C> {
    fn as_raw(&self) -> RawInodeAddressingMode {
        match self {
            Self::Legacy(l) => RawInodeAddressingMode::Legacy(l.clone()),
            Self::Extent(tree) => RawInodeAddressingMode::Extent {
                entries_cnt: tree.entries_cnt,
                max_entries_cnt: tree.max_entries_cnt,
                depth: tree.depth,
                b: tree.b,
            },
        }
    }
}

#[derive(Clone)]
pub enum RawInodeAddressingMode {
    Legacy(Legacy),
    Extent {
        entries_cnt: u16,
        max_entries_cnt: u16,
        depth: u16,
        b: [u8; 48],
    },
}

impl core::fmt::Debug for RawInodeAddressingMode {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Legacy(Legacy { addresses }) => {
                for (idx, addr) in addresses.iter().enumerate() {
                    write!(f, "#{} => {}", idx, addr)?;
                }
            }
            Self::Extent {
                entries_cnt,
                max_entries_cnt,
                depth,
                b,
            } => {
                use crate::inode::extent::{Entry, Internal, Leaf};
                use crate::utils::merge_u32;

                let mut dbg_struct = f.debug_struct("ExtentTree");
                let rw = crate::utils::ByteRw::new(b);
                dbg_struct
                    .field("entries_cnt", entries_cnt)
                    .field("max_entries_cnt", max_entries_cnt)
                    .field("depth", depth);
                for index in 0..*entries_cnt as usize {
                    let node = if *depth == 0 {
                        let (len, is_init) = {
                            let v = rw.read_u16(12 * index + 4);
                            (v & 0x7fff, v < 0x8000)
                        };
                        Entry::Leaf(Leaf {
                            block: FileBlockNumber(rw.read_u32(12 * index)),
                            start: LogicalBlockNumber(merge_u32(
                                rw.read_u16(12 * index + 6) as u32,
                                rw.read_u32(12 * index + 8),
                            )),
                            len,
                            is_init,
                        })
                    } else {
                        Entry::Internal(Internal {
                            block: FileBlockNumber(rw.read_u32(12 * index)),
                            next_node: LogicalBlockNumber(merge_u32(
                                rw.read_u16(12 * index + 8) as u32,
                                rw.read_u32(12 * index + 4),
                            )),
                        })
                    };
                    dbg_struct.field(&alloc::format!("#{}", index), &node);
                }
                dbg_struct.finish()?;
            }
        }
        Ok(())
    }
}

/// Dispatch the inode cursor.
/// the function must returns Result<T, FsError>
macro_rules! dispatch_cursor {
    ($self_:expr, $fs:expr, $fba:expr, |$cursor:ident| $code:expr) => {{
        let rw = $self_.rw.read();
        match &rw.addresses {
            $crate::inode::InodeAddressingMode::Extent(tree) => {
                let $cursor = tree.cursor_from_fba($self_.ino, $fs, $fba)?;
                $code
            }
            $crate::inode::InodeAddressingMode::Legacy(blks) => {
                let $cursor = blks.cursor_from_fba($self_.ino, $fs, $fba)?;
                $code
            }
        }
    }};
    ($self_:expr, $fs:expr, $fba:expr, |mut $cursor:ident| $code:expr) => {{
        let rw = $self_.rw.read();
        match &rw.addresses {
            $crate::inode::InodeAddressingMode::Extent(tree) => {
                #[allow(unused_mut)]
                let mut $cursor = tree.cursor_from_fba($self_.ino, $fs, $fba)?;
                $code
            }
            $crate::inode::InodeAddressingMode::Legacy(blks) => {
                #[allow(unused_mut)]
                let mut $cursor = blks.cursor_from_fba($self_.ino, $fs, $fba)?;
                $code
            }
        }
    }};
}

/// Dispatch the inode cursor.
/// the function must returns Result<T, FsError>
macro_rules! dispatch_cursor_mut {
    ($self_:expr, $fs:expr, $fba:expr, $tx:expr, |$cursor:ident| $code:expr) => {{
        let mut rw = $self_.rw.write();
        match &mut rw.addresses {
            $crate::inode::InodeAddressingMode::Extent(tree) => {
                let $cursor = tree.cursor_from_fba_mut($self_.ino, $fs, $fba, $tx)?;
                $code
            }
            $crate::inode::InodeAddressingMode::Legacy(blks) => {
                let $cursor = blks.cursor_from_fba_mut($self_.ino, $fs, $fba, $tx)?;
                $code
            }
        }
    }};
    ($self_:expr, $fs:expr, $fba:expr, $tx:expr, |mut $cursor:ident| $code:expr) => {{
        let mut rw = $self_.rw.write();
        match &mut rw.addresses {
            $crate::inode::InodeAddressingMode::Extent(tree) => {
                let mut $cursor = tree.cursor_from_fba_mut($self_.ino, $fs, $fba, $tx)?;
                $code
            }
            $crate::inode::InodeAddressingMode::Legacy(blks) => {
                let mut $cursor = blks.cursor_from_fba_mut($self_.ino, $fs, $fba, $tx)?;
                $code
            }
        }
    }};
}

/// Dispatch the inode cursor.
/// the function must returns Result<T, FsError>
macro_rules! dispatch_cursor_last_mut {
    ($self_:expr, $fs:expr, $tx:expr, |$cursor:ident| $code:expr) => {{
        let mut rw = $self_.rw.write();
        match &mut rw.addresses {
            $crate::inode::InodeAddressingMode::Extent(tree) => {
                let $cursor = tree.cursor_last_mut($self_.ino, $fs, $tx)?;
                $code
            }
            $crate::inode::InodeAddressingMode::Legacy(blks) => {
                let $cursor = blks.cursor_last_mut($self_.ino, $fs, $tx)?;
                $code
            }
        }
    }};
    ($self_:expr, $fs:expr, $tx:expr, |mut $cursor:ident| $code:expr) => {{
        let mut rw = $self_.rw.write();
        match &mut rw.addresses {
            $crate::inode::InodeAddressingMode::Extent(tree) => {
                let mut $cursor = tree.cursor_last_mut($self_.ino, $fs, $tx)?;
                $code
            }
            $crate::inode::InodeAddressingMode::Legacy(blks) => {
                let mut $cursor = blks.cursor_last_mut($self_.ino, $fs, $tx)?;
                $code
            }
        }
    }};
}

pub struct InodeRw<C: Config, const BLK_SIZE: usize> {
    pub mode: InodeMode,
    pub size: u64,
    pub links_count: u16,
    pub addresses: InodeAddressingMode<C>,
}

pub struct Inode<C: Config, const BLK_SIZE: usize> {
    pub fs: Weak<FileSystem<C, BLK_SIZE>>,

    // Readonly
    pub(crate) ino: InodeNumber,
    pub ftype: FileType,
    pub flags: InodeFlag,
    pub generation: u32,

    // RW.
    pub(crate) rw: RwLock<InodeRw<C, BLK_SIZE>, C::D>,
}

impl<C: Config, const BLK_SIZE: usize> Drop for Inode<C, BLK_SIZE> {
    fn drop(&mut self) {
        if self.rw.write().links_count == 0 {
            let fs = self.fs.upgrade().unwrap();
            let tx = fs.open_transaction();
            // Truncate to the zero.
            (|| {
                dispatch_cursor_mut!(self, &fs, FileBlockNumber(0), &tx, |mut cursor| cursor
                    .remove_after())
            })()
            .unwrap();
            // Deallocate the inode.
            fs.inodes.deallocate(&fs, self.ino, self.ftype, &tx);

            // Remove inode from orphan list
            fs.sb.remove_orphan(self.ino, &tx);

            tx.done(&fs).unwrap()
        }
    }
}

impl<C: Config, const BLK_SIZE: usize> Inode<C, BLK_SIZE> {
    #[inline]
    pub fn get_fs(&self) -> Option<Arc<FileSystem<C, BLK_SIZE>>> {
        Weak::upgrade(&self.fs)
    }

    #[inline]
    pub fn get_ino(&self) -> InodeNumber {
        self.ino
    }

    pub(crate) fn from_bytes(
        raw: &[u8],
        fs: &Arc<FileSystem<C, BLK_SIZE>>,
        ino: InodeNumber,
        type_: Option<FileType>,
    ) -> Result<Self, FsError> {
        let mut raw_inode = Manipulator::new(raw);

        if raw_inode
            .calculate_checksum(fs, ino)
            .map(|csum| csum == raw_inode.checksum().get())
            .unwrap_or(true)
        {
            if !type_
                .map(|ty| ty == raw_inode.detect_type())
                .unwrap_or(true)
            {
                return Err(FsError::InvalidFs(
                    "Directory entry is corrupted (FileType is differ).",
                ));
            }

            Ok(Inode {
                fs: Arc::downgrade(fs),
                ino,
                ftype: raw_inode.detect_type(),
                flags: InodeFlag::from_bits_truncate(raw_inode.flags().get()),
                generation: raw_inode.generation().get(),

                rw: RwLock::new(InodeRw {
                    size: raw_inode.size(fs).get(),
                    links_count: raw_inode.links_count().get(),
                    mode: InodeMode::from_bits_truncate(raw_inode.mode(fs).get()),
                    addresses: raw_inode.get_addressing_mode(fs),
                }),
            })
        } else {
            Err(FsError::InvalidFs("Failed to verify inode checksum."))
        }
    }

    pub fn new(fs: &Arc<FileSystem<C, BLK_SIZE>>, ino: InodeNumber, ftype: FileType) -> Self {
        let (addresses, mut flags) = if fs
            .sb
            .features_incompatible
            .contains(Ext4FeatureIncompatible::EXTENTS)
            && matches!(ftype, FileType::Directory | FileType::RegularFile)
        {
            (
                InodeAddressingMode::Extent(ExtentTree::default()),
                InodeFlag::EXTENTS,
            )
        } else {
            (
                InodeAddressingMode::Legacy(Legacy::default()),
                InodeFlag::empty(),
            )
        };

        if matches!(ftype, FileType::Directory) {
            flags |= InodeFlag::INDEX;
        }

        Inode {
            fs: Arc::downgrade(fs),
            ino,
            ftype,
            flags,
            generation: 0, // TODO

            rw: RwLock::new(InodeRw {
                size: 0,
                links_count: 1,
                mode: Ext4De::from_file_type(ftype).default_mode(),
                addresses,
            }),
        }
    }

    #[inline]
    pub fn get_size(&self) -> u64 {
        self.rw.read().size
    }

    #[inline]
    pub fn prealloc(
        &self,
        fba: FileBlockNumber,
        fs: &FileSystem<C, BLK_SIZE>,
        tx: &Transaction,
    ) -> Result<(), FsError> {
        if self.rw.read().size / 4096 > fba.0 as u64 {
            // If size is big enough, we don't need to extend the inode
            return Ok(());
        }
        dispatch_cursor_last_mut!(self, fs, tx, |mut c| {
            if let Some(mut rem) = fba.0.checked_sub(c.fba().0) {
                while rem > 0 {
                    let (_, allocated) = c.or_allocated_many(rem, false)?;
                    rem -= allocated;
                    c.move_next_many(allocated)?;
                }
            }
            Ok(())
        })
    }

    #[inline]
    pub fn set_size(&self, size: u64, tx: &Transaction) {
        self.rw.write().size = size;
        tx.inode_set_size(self.ino, size, self.rw.read().addresses.as_raw());
    }

    // FIXME: update path
    #[inline]
    pub fn into_fs_object(self: Arc<Self>) -> crate::FsObject<C, BLK_SIZE> {
        match self.ftype {
            FileType::RegularFile => FsObject::File(File { inode: self }),
            FileType::Directory => FsObject::Directory(Directory { inode: self }),
            _ => todo!("{:?}", self.ftype),
        }
    }
}
