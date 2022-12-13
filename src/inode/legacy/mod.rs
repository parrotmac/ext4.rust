// Rework
mod cursor;

use crate::filesystem::FileSystem;
use crate::transaction::Transaction;
use crate::{Config, FileBlockNumber, FsError, InodeNumber};

pub(crate) use cursor::{Cursor, CursorMut};

#[derive(Debug, Clone, Default)]
pub struct Legacy {
    pub(super) addresses: [u32; 15],
}

impl Legacy {
    pub(crate) fn cursor_last_mut<'a, 'b, 'c, C: Config, const BLK_SIZE: usize>(
        &'a mut self,
        _ino: InodeNumber,
        _fs: &'b FileSystem<C, BLK_SIZE>,
        _tx: &'c Transaction,
    ) -> Result<CursorMut<'a, 'b, 'c, C, BLK_SIZE>, FsError> {
        todo!()
    }

    pub(crate) fn cursor_from_fba<'a, 'b, C: Config, const BLK_SIZE: usize>(
        &'a self,
        _ino: InodeNumber,
        fs: &'b FileSystem<C, BLK_SIZE>,
        fba: FileBlockNumber,
    ) -> Result<Cursor<'a, 'b, C, BLK_SIZE>, FsError> {
        Ok(Cursor {
            fba,
            direct: self,
            _fs: fs,
            _error: None,
        })
    }

    pub(crate) fn cursor_from_fba_mut<'a, 'b, 'c, C: Config, const BLK_SIZE: usize>(
        &'a mut self,
        ino: InodeNumber,
        fs: &'b FileSystem<C, BLK_SIZE>,
        fba: FileBlockNumber,
        tx: &'c Transaction,
    ) -> Result<CursorMut<'a, 'b, 'c, C, BLK_SIZE>, FsError> {
        Ok(CursorMut {
            fba,
            _ino: ino,
            direct: self,
            _fs: fs,
            _tx: tx,
            _error: None,
        })
    }
}
