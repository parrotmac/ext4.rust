use crate::inode::Inode;
use crate::superblock::SuperBlock;
use crate::FsError;
use alloc::boxed::Box;
use alloc::vec::Vec;
use core::ops::{Deref, DerefMut};

macro_rules! make_int_ty {
    ($t:ident, $inner:ty) => {
        #[repr(transparent)]
        #[derive(Clone, Copy, Debug, Ord, PartialOrd, Eq, PartialEq, Hash)]
        pub struct $t(pub $inner);

        impl core::ops::Add<$inner> for $t {
            type Output = Self;

            fn add(self, other: $inner) -> Self::Output {
                Self(self.0 + other)
            }
        }
        impl core::ops::AddAssign<$inner> for $t {
            fn add_assign(&mut self, other: $inner) {
                self.0 = self.0 + other
            }
        }
        impl core::ops::Sub<$inner> for $t {
            type Output = Self;

            fn sub(self, other: $inner) -> Self::Output {
                Self(self.0 - other)
            }
        }
        impl core::ops::SubAssign<$inner> for $t {
            fn sub_assign(&mut self, other: $inner) {
                self.0 = self.0 - other
            }
        }
        impl core::ops::BitOr<$inner> for $t {
            type Output = Self;

            fn bitor(self, other: $inner) -> Self {
                Self(self.0 | other)
            }
        }
        impl core::ops::BitAnd<$inner> for $t {
            type Output = Self;

            fn bitand(self, other: $inner) -> Self {
                Self(self.0 & other)
            }
        }
    };
}

make_int_ty!(FileBlockNumber, u32);
make_int_ty!(LogicalBlockNumber, u64);
make_int_ty!(BlockGroupId, u32);
make_int_ty!(InodeNumber, u32);

impl BlockGroupId {
    #[inline]
    pub fn into_lba_index<C: Config, const BLK_SIZE: usize>(
        self,
        sb: &SuperBlock<C, BLK_SIZE>,
    ) -> (LogicalBlockNumber, usize) {
        // Compute number of descriptors, that fits in one data block
        let dsc_cnt = (BLK_SIZE / sb.block_desc_size) as u32;
        // Block group descriptor table starts at the next block after superblock
        (
            sb.get_descriptor_block(self, dsc_cnt),
            (self.0 % dsc_cnt) as usize * sb.block_desc_size,
        )
    }
}

impl LogicalBlockNumber {
    #[inline]
    pub fn into_bgid_index<C: Config, const BLK_SIZE: usize>(
        self,
        sb: &SuperBlock<C, BLK_SIZE>,
    ) -> Option<(BlockGroupId, usize)> {
        let blocks_per_group = sb.blocks_per_group as u64;
        let baddr = if sb.first_data_block != 0 && self.0 != 0 {
            self.0 - 1
        } else {
            self.0
        };
        let bgid = (baddr / blocks_per_group) as u32;
        if bgid < sb.bg_count {
            Some((
                BlockGroupId((baddr / blocks_per_group) as u32),
                (baddr % blocks_per_group) as usize,
            ))
        } else {
            None
        }
    }

    #[inline]
    pub fn from_bgid_index<C: Config, const BLK_SIZE: usize>(
        bgid: BlockGroupId,
        index: usize,
        sb: &SuperBlock<C, BLK_SIZE>,
    ) -> Self {
        let blocks_per_group = sb.blocks_per_group as u64;
        if sb.first_data_block == 0 {
            Self(blocks_per_group * bgid.0 as u64 + index as u64)
        } else {
            Self(blocks_per_group * bgid.0 as u64 + index as u64 + 1)
        }
    }
}

impl InodeNumber {
    pub fn into_bgid_index<C: Config, const BLK_SIZE: usize>(
        self,
        sb: &SuperBlock<C, BLK_SIZE>,
    ) -> (BlockGroupId, usize) {
        // Inode numbers are 1-based, but it is simpler to work with 0-based when
        // computing indices
        let inodes_per_group = sb.inodes_per_group;
        (
            BlockGroupId((self.0 - 1) / inodes_per_group),
            ((self.0 - 1) % inodes_per_group) as usize,
        )
    }

    #[inline]
    pub fn from_bgid_index<C: Config, const BLK_SIZE: usize>(
        bgid: BlockGroupId,
        index: usize,
        sb: &SuperBlock<C, BLK_SIZE>,
    ) -> Self {
        Self(bgid.0 * sb.inodes_per_group + index as u32 + 1)
    }
}

pub enum FsObject<C: Config, const BLK_SIZE: usize> {
    Directory(crate::directory::Directory<C, BLK_SIZE>),
    File(crate::file::File<C, BLK_SIZE>),
}

impl<C: Config, const BLK_SIZE: usize> FsObject<C, BLK_SIZE> {
    #[inline]
    pub fn get_directory(self) -> Option<crate::directory::Directory<C, BLK_SIZE>> {
        if let Self::Directory(d) = self {
            Some(d)
        } else {
            None
        }
    }

    #[inline]
    pub fn get_file(self) -> Option<crate::file::File<C, BLK_SIZE>> {
        if let Self::File(f) = self {
            Some(f)
        } else {
            None
        }
    }

    #[inline]
    pub fn get_inode(&self) -> &Inode<C, BLK_SIZE> {
        match self {
            Self::Directory(d) => &d.inode,
            Self::File(f) => &f.inode,
        }
    }
}

pub trait Zero {
    fn zeroed() -> Self;
}

impl<const N: usize> Zero for Box<[u8; N]> {
    fn zeroed() -> Self {
        Box::new([0; N])
    }
}

pub trait Config
where
    Self: Send + Sync,
{
    type D: crate::RwDreamer;
    type S: crate::Dreamer;
    type Buffer<const N: usize>: Deref<Target = [u8; N]> + DerefMut + Zero + Send + Sync + Clone;

    fn read_bytes<const N: usize>(&self, ofs: usize) -> Result<Self::Buffer<N>, FsError>;
    fn write_bytes<const N: usize>(&self, ofs: usize, buf: &Self::Buffer<N>)
        -> Result<(), FsError>;

    fn read_vectored<B: Extend<Self::Buffer<N>>, const N: usize>(
        &self,
        b: &mut B,
        ofs: usize,
        chunks: usize,
    ) -> Result<(), FsError> {
        for i in 0..chunks {
            b.extend(Some(self.read_bytes::<N>(ofs + N * i)?));
        }
        Ok(())
    }

    fn write_vectored<const N: usize>(
        &self,
        mut ofs: usize,
        bufs: &[&Self::Buffer<N>],
    ) -> Result<(), FsError> {
        for buf in bufs {
            self.write_bytes(ofs, buf)?;
            ofs += buf.as_ref().len();
        }
        Ok(())
    }

    fn write_bios<const N: usize>(
        &self,
        bio: Vec<(usize, Self::Buffer<N>)>,
    ) -> Result<(), FsError> {
        for (ofs, buf) in bio.into_iter() {
            self.write_bytes(ofs, &buf)?;
        }
        Ok(())
    }

    fn total_size(&self) -> usize;
}

#[cfg(all(any(feature = "std", test), target_family = "unix"))]
impl Config for std::fs::File {
    type D = crate::prelude::OpaqueDreamer;
    type S = crate::prelude::SpinningDreamer;
    type Buffer<const N: usize> = Box<[u8; N]>;

    fn read_bytes<const N: usize>(&self, ofs: usize) -> Result<Box<[u8; N]>, FsError> {
        use std::os::unix::fs::FileExt;
        let mut b = Box::new([0; N]);
        self.read_at(b.as_mut(), ofs as u64)
            .map_err(|_| FsError::IoError)
            .map(|_| b)
    }
    fn write_bytes<const N: usize>(
        &self,
        ofs: usize,
        buf: &Self::Buffer<N>,
    ) -> Result<(), FsError> {
        use std::os::unix::fs::FileExt;
        self.write_at(buf.as_ref(), ofs as u64)
            .map_err(|_| FsError::IoError)
            .map(|_| ())?;
        self.sync_data().map_err(|_| FsError::IoError)
    }
    fn write_vectored<const N: usize>(
        &self,
        mut ofs: usize,
        bufs: &[&Self::Buffer<N>],
    ) -> Result<(), FsError> {
        for buf in bufs {
            self.write_bytes(ofs, buf)?;
            ofs += buf.as_ref().len();
        }
        self.sync_data().map_err(|_| FsError::IoError)?;
        Ok(())
    }

    fn total_size(&self) -> usize {
        use std::io::{Seek, SeekFrom};
        use std::os::unix::fs::MetadataExt;

        let meta = self.metadata().unwrap();
        if meta.rdev() == 0 {
            meta.len() as usize
        } else {
            self.try_clone().unwrap().seek(SeekFrom::End(0)).unwrap() as usize
        }
    }
}
