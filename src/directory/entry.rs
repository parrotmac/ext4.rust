//! Ext4 directory entry structure.
//!
//! #[repr(C)]
//! struct DirectoryEntryV1 {
//!     inode: LeU32,
//!     entry_len: LeU16,
//!     name_len: LeU16,
//!     name: [u8; 0],
//! }
//!
//! // if sb.rev_level > 0 || sb.minor_rev_level >= 5 then below structure is
//! used. #[repr(C)]
//! struct DirectoryEntryV2 {
//!     inode: LeU32,
//!     entry_len: LeU16,
//!     name_len: u8,
//!     file_type: u8,
//!     name: [u8; 0],
//! }

use crate::inode::Ext4De;
use crate::utils::ByteRw;
use crate::InodeNumber;
use core::convert::{TryFrom, TryInto};

pub(crate) enum DirectoryEntryDispatch<T>
where
    T: core::convert::AsRef<[u8]>,
{
    V1(ByteRw<T>),
    V2(ByteRw<T>),
}

impl<'a> DirectoryEntryDispatch<&'a [u8]> {
    #[inline]
    pub fn get_name_for_sort(self) -> &'a str {
        let name_len = self.get_name_len();
        match self {
            Self::V1(r) | Self::V2(r) => core::str::from_utf8(&r.into_inner()[8..8 + name_len]),
        }
        .unwrap()
    }
}

impl<T> DirectoryEntryDispatch<T>
where
    T: core::convert::AsRef<[u8]>,
{
    #[inline]
    pub fn get_inode(&self) -> InodeNumber {
        match self {
            Self::V1(r) | Self::V2(r) => InodeNumber(r.read_u32(0)),
        }
    }

    #[inline]
    pub fn get_entry_len(&self) -> usize {
        match self {
            Self::V1(r) | Self::V2(r) => r.read_u16(4) as usize,
        }
    }

    #[inline]
    pub fn get_name_len(&self) -> usize {
        match self {
            Self::V1(r) => r.read_u16(6) as usize,
            Self::V2(r) => r.read_u8(6) as usize,
        }
    }

    #[inline]
    pub fn get_name(&self) -> &str {
        match self {
            Self::V1(r) | Self::V2(r) => {
                core::str::from_utf8(&r.inner().as_ref()[8..8 + self.get_name_len()])
            }
        }
        .unwrap()
    }

    #[inline]
    pub fn get_file_type(&self) -> Option<Ext4De> {
        match self {
            Self::V1(_) => None,
            Self::V2(r) => Ext4De::try_from(r.read_u8(7)).ok(),
        }
    }
}

impl<'a> DirectoryEntryDispatch<&'a mut [u8]> {
    #[inline]
    pub fn split(mut self) -> DirectoryEntryDispatch<&'a mut [u8]> {
        let name_len = self.get_name_len();
        let entry_len = self.get_entry_len();
        let new_len = (11 + name_len) & !3;
        self.set_entry_len(new_len as u16);

        let mut new = match self {
            Self::V1(r) => Self::V1(ByteRw::new(&mut r.into_inner()[new_len..])),
            Self::V2(r) => Self::V2(ByteRw::new(&mut r.into_inner()[new_len..])),
        };
        new.set_entry_len((entry_len - new_len) as u16);
        new
    }

    #[inline]
    pub fn set_inode(&mut self, inode: InodeNumber) -> &mut Self {
        match self {
            Self::V1(r) | Self::V2(r) => r.write_u32(0, inode.0),
        }
        self
    }

    #[inline]
    pub fn set_entry_len(&mut self, entry_len: u16) -> &mut Self {
        match self {
            Self::V1(r) | Self::V2(r) => r.write_u16(4, entry_len),
        }
        self
    }

    #[inline]
    pub fn set_name(&mut self, name: &str) -> &mut Self {
        match self {
            Self::V1(r) => {
                r.write_u16(6, name.len().try_into().unwrap());
                r.inner_mut()[8..8 + name.len()].copy_from_slice(name.as_bytes());
            }
            Self::V2(r) => {
                r.write_u8(6, name.len().try_into().unwrap());
                r.inner_mut()[8..8 + name.len()].copy_from_slice(name.as_bytes());
            }
        }
        self
    }

    #[inline]
    pub fn set_type(&mut self, de: Option<Ext4De>) -> &mut Self {
        match self {
            Self::V1(_) => (),
            Self::V2(r) => r.write_u8(7, de.unwrap() as u8),
        }
        self
    }
}
