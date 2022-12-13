use super::entry::DirectoryEntryDispatch;
use crate::filesystem::FileSystem;
use crate::inode::Ext4De;
use crate::utils::ByteRw;
use crate::{Config, FileType, FsError, InodeNumber};

pub struct DirectoryBlock<T, const BLK_SIZE: usize>
where
    T: core::ops::Deref<Target = [u8; BLK_SIZE]>,
{
    inner: T,
    is_version2: bool,
}

impl<T, const BLK_SIZE: usize> DirectoryBlock<T, BLK_SIZE>
where
    T: core::ops::Deref<Target = [u8; BLK_SIZE]>,
{
    #[inline]
    pub fn new<C: Config>(inner: T, fs: &FileSystem<C, BLK_SIZE>) -> Self {
        Self {
            inner,
            is_version2: fs.sb.is_dir_entry_version2,
        }
    }
}

impl<T, const BLK_SIZE: usize> DirectoryBlock<T, BLK_SIZE>
where
    T: core::ops::Deref<Target = [u8; BLK_SIZE]>,
{
    #[inline]
    fn read_entry_at(&self, pos: usize) -> Result<DirectoryEntryDispatch<&[u8]>, ()> {
        let entry = if self.is_version2 {
            DirectoryEntryDispatch::V2(ByteRw::new(&self.inner[pos..]))
        } else {
            DirectoryEntryDispatch::V1(ByteRw::new(&self.inner[pos..]))
        };
        if BLK_SIZE - pos - 8 < entry.get_name_len() {
            Err(())
        } else {
            Ok(entry)
        }
    }

    #[inline]
    pub(crate) fn iter(&self) -> DirectoryBlockIterator<T, BLK_SIZE> {
        DirectoryBlockIterator {
            inner: self,
            pos: 0,
            stopped: false,
        }
    }
}

// Mutable
impl<T, const BLK_SIZE: usize> DirectoryBlock<T, BLK_SIZE>
where
    T: core::ops::Deref<Target = [u8; BLK_SIZE]>,
    T: core::ops::DerefMut,
{
    pub(super) fn clear(&mut self) {
        let mut rw = ByteRw::new(self.inner.as_mut());
        rw.write_u32(0, 0);
        rw.write_u16(4, BLK_SIZE as u16);
        rw.write_u16(0, 0);
    }

    pub(super) fn insert_entry(
        &mut self,
        entry: &str,
        de: Option<FileType>,
        child: InodeNumber,
    ) -> Result<bool, FsError> {
        // XXX: does we need to preserve original content for roll-back the operation on
        // failure?
        let entry_size = (11 + entry.len()) & !3;

        let mut iter_mut = self.iter_mut();
        while let Some(e) = iter_mut.next() {
            let mut cursor = e?;
            let en = cursor.current_mut();
            let mut target = if en.get_inode().0 == 0
                && !matches!(en.get_file_type(), Some(Ext4De::CheckSum))
                && en.get_entry_len() >= entry_size
            {
                en
            } else if en.get_inode().0 != 0
                && en.get_entry_len() >= entry_size + ((11 + en.get_name_len()) & !3)
            {
                en.split()
            } else {
                continue;
            };
            target
                .set_inode(child)
                .set_type(de.map(Ext4De::from_file_type))
                .set_name(entry);
            return Ok(true);
        }
        Ok(false)
    }

    #[inline]
    pub fn iter_mut(&mut self) -> DirectoryBlockIteratorMut<T, BLK_SIZE> {
        DirectoryBlockIteratorMut {
            block: self,
            pos: 0,
            ppos: None,
            stopped: false,
        }
    }
}

pub(crate) struct DirectoryBlockIterator<'a, T, const BLK_SIZE: usize>
where
    T: core::ops::Deref<Target = [u8; BLK_SIZE]>,
{
    inner: &'a DirectoryBlock<T, BLK_SIZE>,
    pos: usize,
    stopped: bool,
}

impl<'a, T, const BLK_SIZE: usize> DirectoryBlockIterator<'a, T, BLK_SIZE>
where
    T: core::ops::Deref<Target = [u8; BLK_SIZE]>,
{
    #[inline]
    pub fn pos(&self) -> usize {
        self.pos
    }
}

impl<'a, T, const BLK_SIZE: usize> core::iter::Iterator for DirectoryBlockIterator<'a, T, BLK_SIZE>
where
    T: core::ops::Deref<Target = [u8; BLK_SIZE]>,
{
    type Item = Result<DirectoryEntryDispatch<&'a [u8]>, FsError>;

    fn next(&mut self) -> Option<Self::Item> {
        while !self.stopped && self.pos + 8 <= BLK_SIZE {
            if let Ok(entry) = self.inner.read_entry_at(self.pos) {
                self.pos += entry.get_entry_len() as usize;
                if entry.get_inode() != crate::types::InodeNumber(0) {
                    return Some(Ok(entry));
                }
            } else {
                self.stopped = true;
                return Some(Err(FsError::Fixme));
            }
        }
        None
    }
}

pub struct DirectoryBlockIteratorMut<'a, T, const BLK_SIZE: usize>
where
    T: core::ops::Deref<Target = [u8; BLK_SIZE]>,
    T: core::ops::DerefMut,
{
    block: &'a mut DirectoryBlock<T, BLK_SIZE>,
    pos: usize,
    ppos: Option<usize>,
    stopped: bool,
}

pub struct DirectoryBlockIteratorMutCursor<'a, T, const BLK_SIZE: usize>
where
    T: core::ops::Deref<Target = [u8; BLK_SIZE]>,
    T: core::ops::DerefMut,
{
    block: &'a mut DirectoryBlock<T, BLK_SIZE>,
    ppos: Option<usize>,
    pos: usize,
}

impl<'a, T, const BLK_SIZE: usize> DirectoryBlockIteratorMutCursor<'a, T, BLK_SIZE>
where
    T: core::ops::Deref<Target = [u8; BLK_SIZE]>,
    T: core::ops::DerefMut,
{
    fn get_en(&self, pos: usize) -> DirectoryEntryDispatch<&[u8]> {
        if self.block.is_version2 {
            DirectoryEntryDispatch::V2(ByteRw::new(&self.block.inner[pos..]))
        } else {
            DirectoryEntryDispatch::V1(ByteRw::new(&self.block.inner[pos..]))
        }
    }

    fn get_en_mut(&mut self, pos: usize) -> DirectoryEntryDispatch<&mut [u8]> {
        if self.block.is_version2 {
            DirectoryEntryDispatch::V2(ByteRw::new(&mut self.block.inner[pos..]))
        } else {
            DirectoryEntryDispatch::V1(ByteRw::new(&mut self.block.inner[pos..]))
        }
    }

    pub(crate) fn current(&self) -> DirectoryEntryDispatch<&[u8]> {
        self.get_en(self.pos)
    }

    pub(crate) fn current_mut(&mut self) -> DirectoryEntryDispatch<&mut [u8]> {
        self.get_en_mut(self.pos)
    }

    pub fn destroy(mut self) -> InodeNumber {
        let mut cur = self.get_en_mut(self.pos);
        let (cur_inode, cur_en_len) = (cur.get_inode(), cur.get_entry_len());
        cur.set_inode(InodeNumber(0));
        if let Some(ppos) = self.ppos {
            let mut prev = self.get_en_mut(ppos);
            prev.set_entry_len((prev.get_entry_len() + cur_en_len) as u16);
        }
        cur_inode
    }
}

impl<'a, T, const BLK_SIZE: usize> DirectoryBlockIteratorMut<'a, T, BLK_SIZE>
where
    T: core::ops::Deref<Target = [u8; BLK_SIZE]>,
    T: core::ops::DerefMut,
{
    pub fn next(
        &mut self,
    ) -> Option<Result<DirectoryBlockIteratorMutCursor<T, BLK_SIZE>, FsError>> {
        if !self.stopped && self.pos + 8 <= BLK_SIZE {
            let entry = if self.block.is_version2 {
                DirectoryEntryDispatch::V2(ByteRw::new(&mut self.block.inner[self.pos..]))
            } else {
                DirectoryEntryDispatch::V1(ByteRw::new(&mut self.block.inner[self.pos..]))
            };
            if self.pos + entry.get_entry_len() <= BLK_SIZE
                && entry.get_name_len() + 8 <= entry.get_entry_len()
            {
                let (ppos, pos) = (self.ppos, self.pos);
                self.ppos = Some(self.pos);
                self.pos += entry.get_entry_len() as usize;
                Some(Ok(DirectoryBlockIteratorMutCursor {
                    block: self.block,
                    ppos,
                    pos,
                }))
            } else {
                self.stopped = true;
                Some(Err(FsError::InvalidFs("Directory block is corrupted")))
            }
        } else {
            None
        }
    }
}
