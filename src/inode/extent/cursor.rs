use super::{ExtentTree, Leaf, Path};
use crate::filesystem::FileSystem;
use crate::inode::AddressingOutput;
use crate::transaction::Transaction;
use crate::{Config, FileBlockNumber, FsError, InodeNumber, LogicalBlockNumber};
use core::convert::TryInto;

pub(crate) struct Cursor<'a, 'b, 'c, C: Config, const BLK_SIZE: usize> {
    pub(super) fba: FileBlockNumber,
    pub(super) path: Path<'b, 'c, &'a ExtentTree<C>, C, BLK_SIZE, false>,
    pub(super) fs: &'b FileSystem<C, BLK_SIZE>,
    pub _error: Option<FsError>,
}

impl<'a, 'b, 'c, C: Config, const BLK_SIZE: usize> Cursor<'a, 'b, 'c, C, BLK_SIZE> {
    // Get the logical block number pointed by the cursor.
    #[inline]
    pub(crate) fn current(&mut self) -> Option<AddressingOutput> {
        let Leaf {
            block,
            start,
            len,
            is_init,
        } = self.path.get_leaf()?;
        if block <= self.fba && self.fba < block + (len as u32) {
            if is_init {
                Some(AddressingOutput::Initialized(
                    start + ((self.fba.0 - block.0) as u64),
                ))
            } else {
                Some(AddressingOutput::Uninitialized)
            }
        } else {
            None
        }
    }

    /// Move the fba to the next + 1.
    #[inline]
    pub(crate) fn move_next(&mut self) -> Result<(), FsError> {
        if let Some(Leaf { block, len, .. }) = self.path.get_leaf() {
            self.fba += 1;
            if block > self.fba || self.fba >= block + (len as u32) {
                self.path.load(self.fba, self.fs)?;
            }
        }
        Ok(())
    }
}

impl<'a, 'b, 'c, C: Config, const BLK_SIZE: usize> core::iter::Iterator
    for Cursor<'a, 'b, 'c, C, BLK_SIZE>
{
    type Item = AddressingOutput;

    fn next(&mut self) -> Option<Self::Item> {
        let o = self.current();
        if o.is_some() {
            if let Err(e) = self.move_next() {
                self._error = Some(e);
                return None;
            }
        }
        o
    }
}

// Mutable cursor
pub(crate) struct CursorMut<'a, 'b, 'c, C: Config, const BLK_SIZE: usize> {
    pub(super) fba: FileBlockNumber,
    pub(super) ino: InodeNumber,
    pub(super) path: Path<'b, 'c, &'a mut ExtentTree<C>, C, BLK_SIZE, true>,
    pub(super) fs: &'b FileSystem<C, BLK_SIZE>,
    pub(super) tx: &'c Transaction,
    pub _error: Option<FsError>,
}

impl<'a, 'b, 'c, C: Config, const BLK_SIZE: usize> CursorMut<'a, 'b, 'c, C, BLK_SIZE>
where
    'b: 'c,
{
    #[inline]
    pub fn fba(&self) -> FileBlockNumber {
        self.fba
    }

    // Get the logical block number pointed by the cursor.
    #[inline]
    pub(crate) fn current(&mut self) -> Option<AddressingOutput> {
        let Leaf {
            block,
            start,
            len,
            is_init,
        } = self.path.get_leaf()?;
        if block <= self.fba && self.fba < block + (len as u32) {
            if is_init {
                Some(AddressingOutput::Initialized(
                    start + ((self.fba.0 - block.0) as u64),
                ))
            } else {
                Some(AddressingOutput::Uninitialized)
            }
        } else {
            None
        }
    }

    #[inline]
    pub(crate) fn or_allocated_many(
        &mut self,
        size: u32,
        is_meta: bool,
    ) -> Result<(LogicalBlockNumber, u32), FsError> {
        let hope = if let Some(Leaf {
            block,
            start,
            len,
            is_init,
        }) = self.path.get_leaf()
        {
            match ((block..block + (len as u32)).contains(&self.fba), is_init) {
                (true, true) => return Ok((start + ((self.fba.0 - block.0) as u64), 1)),
                (_, false) => todo!("Uninitialized extent"),
                (false, true) if block > self.fba => start + (self.fba.0 as u64 - start.0 as u64),
                (false, true) => start - (self.fba.0 as u64 - block.0 as u64),
            }
        } else if self.path.leafs.is_empty() {
            // tree is empty.
            LogicalBlockNumber(((self.ino.0 - 1) / self.fs.sb.inodes_per_group) as u64)
        } else {
            // node is empty.
            self.path.leafs.last().unwrap().0.b.lba()
        };

        let (leaf, len) = self
            .fs
            .blocks
            .allocate(self.ino, size as usize, hope, self.fs, self.tx)
            .map(|(lba, len)| {
                (
                    Leaf {
                        block: self.fba,
                        start: lba,
                        len: len.try_into().unwrap(),
                        is_init: true,
                    },
                    len,
                )
            })?;
        let n = self.path.insert(leaf, self.ino, self.tx, self.fs)?;
        if is_meta {
            for i in 0..len {
                self.fs
                    .blocks
                    .get_mut_noload(n + (i as u64), &self.tx.collector)?;
            }
        }

        Ok((n, len as u32))
    }

    #[inline]
    pub(crate) fn or_allocated(&mut self, is_meta: bool) -> Result<LogicalBlockNumber, FsError> {
        self.or_allocated_many(1, is_meta).map(|(n, _)| n)
    }

    /// Move the cursor to fba + n.
    pub(crate) fn move_next_many(&mut self, n: u32) -> Result<(), FsError> {
        if let Some(l) = self.path.get_leaf() {
            self.fba += n;
            if !l.contains(self.fba) {
                self.path.load(self.fba, self.tx, self.fs)?;
            }
        }
        Ok(())
    }

    /// Move the cursor to fba + 1.
    #[inline]
    pub(crate) fn move_next(&mut self) -> Result<(), FsError> {
        self.move_next_many(1)
    }

    // Remove every entry after the cursor (truncate).
    pub(crate) fn remove_after(&mut self) -> Result<(), FsError> {
        let until = self.fba;
        self.path.load_path_to_last(self.tx, self.fs)?;
        while let Some(l) = self.path.get_leaf() {
            // fblock .. fblock + len -> fblock .. until
            if l.block + (l.len as u32) < until {
                break;
            }
            self.path.truncate_last(
                until
                    .0
                    .checked_sub(l.block.0)
                    .unwrap_or_default()
                    .try_into()
                    .unwrap(),
                self.tx,
                self.fs,
            )?;
        }
        Ok(())
    }
}

impl<'a, 'b, 'c, C: Config, const BLK_SIZE: usize> core::iter::Iterator
    for CursorMut<'a, 'b, 'c, C, BLK_SIZE>
where
    'b: 'c,
{
    type Item = AddressingOutput;

    fn next(&mut self) -> Option<Self::Item> {
        let o = self.current();
        if o.is_some() {
            if let Err(e) = self.move_next() {
                self._error = Some(e);
                return None;
            }
        }
        o
    }
}

impl<'a, 'b, 'c, C: Config, const BLK_SIZE: usize> Drop for Cursor<'a, 'b, 'c, C, BLK_SIZE> {
    fn drop(&mut self) {
        #[cfg(feature = "extent_cache")]
        if let Some(leaf) = self.path.get_leaf() {
            if let Ok(mut o) = self.path.root.0.cache.try_write() {
                *o = Some(leaf);
            }
        }
    }
}

impl<'a, 'b, 'c, C: Config, const BLK_SIZE: usize> Drop for CursorMut<'a, 'b, 'c, C, BLK_SIZE> {
    fn drop(&mut self) {
        #[cfg(feature = "extent_cache")]
        if let Some(leaf) = self.path.get_leaf() {
            if let Ok(mut o) = self.path.root.0.cache.try_write() {
                *o = Some(leaf);
            }
        }
    }
}
