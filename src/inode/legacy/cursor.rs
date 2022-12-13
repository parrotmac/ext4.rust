use super::Legacy;
use crate::filesystem::FileSystem;
use crate::inode::AddressingOutput;
use crate::transaction::Transaction;
use crate::{Config, FileBlockNumber, FsError, InodeNumber, LogicalBlockNumber};

pub(crate) struct Cursor<'a, 'b, C: Config, const BLK_SIZE: usize> {
    pub(super) fba: FileBlockNumber,
    pub(super) direct: &'a Legacy,
    pub(super) _fs: &'b FileSystem<C, BLK_SIZE>,
    pub _error: Option<FsError>,
}

impl<'a, 'b, C: Config, const BLK_SIZE: usize> Cursor<'a, 'b, C, BLK_SIZE> {
    // Get the logical block number pointed by the cursor.
    pub(crate) fn current(&mut self) -> Option<AddressingOutput> {
        if self.fba < FileBlockNumber(12) {
            Some(AddressingOutput::Initialized(LogicalBlockNumber(
                self.direct.addresses[self.fba.0 as usize] as u64,
            )))
        } else {
            todo!()
        }
    }

    /// Move the fba to the next + 1.
    pub(crate) fn move_next(&mut self) -> Result<(), FsError> {
        self.fba += 1;
        if self.fba > FileBlockNumber(12) {
            todo!()
        }
        Ok(())
    }
}

impl<'a, 'b, C: Config, const BLK_SIZE: usize> core::iter::Iterator
    for Cursor<'a, 'b, C, BLK_SIZE>
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

pub(crate) struct CursorMut<'a, 'b, 'c, C: Config, const BLK_SIZE: usize> {
    pub(super) fba: FileBlockNumber,
    pub(super) _ino: InodeNumber,
    pub(super) direct: &'a Legacy,
    pub(super) _fs: &'b FileSystem<C, BLK_SIZE>,
    pub(super) _tx: &'c Transaction,
    pub _error: Option<FsError>,
}

impl<'a, 'b, 'c, C: Config, const BLK_SIZE: usize> CursorMut<'a, 'b, 'c, C, BLK_SIZE> {
    #[inline]
    pub fn fba(&self) -> FileBlockNumber {
        self.fba
    }

    // Get the logical block number pointed by the cursor.
    pub(crate) fn current(&self) -> Option<AddressingOutput> {
        if self.fba < FileBlockNumber(12) {
            Some(AddressingOutput::Initialized(LogicalBlockNumber(
                self.direct.addresses[self.fba.0 as usize] as u64,
            )))
        } else {
            todo!()
        }
    }

    pub(crate) fn move_next_many(&mut self, _n: u32) -> Result<(), FsError> {
        todo!()
    }

    /// Move the fba to the next + 1.
    pub(crate) fn move_next(&mut self) -> Result<(), FsError> {
        self.fba += 1;
        if self.fba > FileBlockNumber(12) {
            todo!()
        }
        Ok(())
    }

    #[inline]
    pub(crate) fn or_allocated_many(
        &mut self,
        _size: u32,
        _is_meta: bool,
    ) -> Result<(LogicalBlockNumber, u32), FsError> {
        todo!()
    }

    pub(crate) fn or_allocated(&mut self, is_meta: bool) -> Result<LogicalBlockNumber, FsError> {
        if let Some(blk) = self.current() {
            Ok(blk.get_initialized().unwrap())
        } else {
            self.or_allocated_many(1, is_meta).map(|(n, _)| n)
        }
    }

    // Remove every entry after the cursor (truncate).
    pub(crate) fn remove_after(&mut self) -> Result<(), FsError> {
        todo!()
    }
}

impl<'a, 'b, 'c, C: Config, const BLK_SIZE: usize> core::iter::Iterator
    for CursorMut<'a, 'b, 'c, C, BLK_SIZE>
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
