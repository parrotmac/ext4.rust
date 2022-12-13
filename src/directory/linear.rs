use super::{Directory, DirectoryBlock};
use crate::directory::entry::DirectoryEntryDispatch;
use crate::filesystem::FileSystem;
use crate::transaction::{Collector, Transaction};
use crate::{Config, FileBlockNumber, FileType, FsError, InodeNumber};
use fs_core::DirEntry;
use path::Path;

pub struct LinearScheme<'a, C: Config, const BLK_SIZE: usize> {
    dir: &'a Directory<C, BLK_SIZE>,
}

impl<'a, C: Config, const BLK_SIZE: usize> LinearScheme<'a, C, BLK_SIZE> {
    #[inline]
    pub fn new(dir: &'a Directory<C, BLK_SIZE>) -> Self {
        Self { dir }
    }

    pub fn find_entry(
        &self,
        fs: &FileSystem<C, BLK_SIZE>,
        target: &str,
    ) -> Result<(InodeNumber, Option<FileType>), FsError> {
        dispatch_cursor!(self.dir.inode, fs, FileBlockNumber(0), |mut cursor| {
            loop {
                let lba = cursor.current().ok_or(FsError::NoEntry).and_then(|n| {
                    n.get_initialized()
                        .ok_or(FsError::InvalidFs("Uninitialized data block in directory"))
                })?;
                let blk = fs.blocks.get(lba)?;

                for entry in DirectoryBlock::new(&**blk.read(), fs).iter() {
                    let entry = entry?;
                    if entry.get_name() == target && entry.get_inode().0 != 0 {
                        return Ok((
                            entry.get_inode(),
                            entry.get_file_type().map(|n| n.into_file_type()),
                        ));
                    }
                }
                cursor.move_next()?;
            }
        });
    }

    pub fn read_dir(
        &self,
        fs: &FileSystem<C, BLK_SIZE>,
        pos: usize,
    ) -> Result<Option<(DirEntry, usize)>, FsError> {
        let (index, offset) = (pos / BLK_SIZE, pos % BLK_SIZE);

        if let Some(lba) = dispatch_cursor!(
            self.dir.inode,
            fs,
            FileBlockNumber(index as u32),
            |mut c| c.current()
        )
        .map(|n| {
            n.get_initialized()
                .ok_or(FsError::InvalidFs("Uninitialized data block in directory"))
        }) {
            let blk = fs.blocks.get(lba?)?;
            let guard = blk.read();
            let mut pos = 0;
            let mut pen: Option<DirectoryEntryDispatch<&[u8]>> = None;
            let dblk = DirectoryBlock::new(&**guard, fs);
            for en in dblk.iter() {
                if pos <= offset {
                    pen = Some(en?);
                    pos += pen.as_ref().unwrap().get_entry_len();
                } else {
                    break;
                }
            }
            if let Some(pen) = pen {
                Ok(Some((
                    DirEntry {
                        path: Path::new(pen.get_name()).to_path_buf(),
                        ino: fs_core::InodeNumber(pen.get_inode().0 as u64),
                        ty: pen
                            .get_file_type()
                            .map(|n| n.into_file_type())
                            .unwrap_or(FileType::Unknown),
                    },
                    pos + index * BLK_SIZE,
                )))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    pub fn add_entry(
        &self,
        fs: &FileSystem<C, BLK_SIZE>,
        entry: &str,
        de: FileType,
        child: InodeNumber,
        tx: &Transaction,
    ) -> Result<(), FsError> {
        // BUG: add entry is not reflected.
        let mut grown;
        let o = dispatch_cursor_mut!(self.dir.inode, fs, FileBlockNumber(0), tx, |mut c| {
            loop {
                grown = c.current().is_none();
                let lba = c.or_allocated(true)?;
                let blk = fs.blocks.get_mut(lba, &tx.collector)?;
                let mut guard = blk.write();
                let mut dblk = DirectoryBlock::new(&mut **guard, fs);
                if grown {
                    dblk.clear();
                }
                if dblk.insert_entry(entry, Some(de), child)? {
                    break Ok(());
                }
                c.move_next()?;
            }
        });
        if grown {
            self.dir
                .inode
                .set_size(self.dir.inode.get_size() + BLK_SIZE as u64, tx);
        }
        o
    }

    pub fn has_child(&self, _fs: &FileSystem<C, BLK_SIZE>) -> Result<bool, FsError> {
        todo!()
    }

    pub fn remove_entry(
        &self,
        fs: &FileSystem<C, BLK_SIZE>,
        target: &str,
        collector: &Collector,
    ) -> Result<InodeNumber, FsError> {
        dispatch_cursor!(self.dir.inode, fs, FileBlockNumber(0), |mut cursor| {
            loop {
                let lba = cursor
                    .current()
                    .ok_or(FsError::NoEntry)?
                    .get_initialized()
                    .ok_or(FsError::InvalidFs("Uninitialized data block in directory"))?;
                let blk = fs.blocks.get_mut(lba, collector)?;
                let mut guard = blk.write();
                let mut dblk = DirectoryBlock::new(&mut **guard, fs);
                let mut iter = dblk.iter_mut();
                while let Some(entry) = iter.next() {
                    let entry = entry?;
                    let en = entry.current();
                    if en.get_name() == target && en.get_inode().0 != 0 {
                        return Ok(entry.destroy());
                    }
                }
                cursor.move_next()?;
            }
        });
    }
}
