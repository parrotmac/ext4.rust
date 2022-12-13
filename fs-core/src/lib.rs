#![no_std]
use path::PathBuf;

/// File system errors.
#[derive(PartialEq, Eq, Debug, Clone, Copy)]
pub enum FsError {
    /// File system state is invalid.
    InvalidFs(&'static str),
    /// IO Error.
    IoError,
    /// No such entry.
    NoEntry,
    /// File exceeds the maximum file size.
    Big,
    /// Target is not file.
    NotFile,
    /// Target is not directory.
    NotDirectory,
    /// Directory is not empty.
    NotEmpty,
    /// File Exist.
    Exist,
    /// File system is full.
    FsFull,
    /// File system is shutdowned.
    Shutdown,
    /// Invalid argument is given.
    Invalid,
    /// Operation is not permitted.
    PermissionDenied,
    /// Unsupported
    Unsupported(&'static str),
    /// File is busy. (e.g. wait for lazy mapping)
    Busy,
    /// Remove request is queued.
    PendingRemoval,
    /// FIXME
    Fixme,
}

/// File types.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
pub enum FileType {
    /// Unknown file type.
    Unknown = 0,
    /// Regular file type.
    RegularFile = 1,
    /// Directory.
    Directory = 2,
    /// Character device.
    CharacterDev = 3,
    /// Block device.
    BlockDev = 4,
    /// Fifo object.
    Fifo = 5,
    /// Socket object.
    Socket = 6,
    /// Symbolic link.
    Symlink = 7,
}

impl FileType {
    #[inline]
    pub fn to_u8(self) -> u8 {
        self as u8
    }
    #[inline]
    pub fn from_u8(v: u8) -> Self {
        match v {
            1 => Self::RegularFile,
            2 => Self::Directory,
            3 => Self::CharacterDev,
            4 => Self::BlockDev,
            5 => Self::Fifo,
            6 => Self::Socket,
            7 => Self::Symlink,
            _ => Self::Unknown,
        }
    }
}

bitflags::bitflags! {
    /// Mode of the inodes.
    pub struct InodeMode: u32 {
        /// Others may execute
        const XOTH = 0o1;
        /// Others may write
        const WOTH = 0o2;
        /// Others may read
        const ROTH = 0o4;
        /// Group members may execute
        const XGRP = 0o10;
        /// Group members may write
        const WGRP = 0o20;
        /// Group members may read
        const RGRP = 0o40;
        /// Owner may execute
        const XUSR = 0o100;
        /// Owner may write
        const WUSR = 0o200;
        /// Owner may read
        const RUSR = 0o400;
        /// Sticky bit
        const SVTX = 0o1000;
        /// Setgid
        const SGID = 0o2000;
        /// Setuid
        const SUID = 0o4000;
    }
}

/// Inode number.
#[derive(Copy, Clone, Eq, PartialEq, Debug, Hash, Ord, PartialOrd)]
pub struct InodeNumber(pub u64);

#[derive(Debug)]
pub struct DirEntry {
    pub path: PathBuf,
    pub ino: InodeNumber,
    pub ty: FileType,
}
