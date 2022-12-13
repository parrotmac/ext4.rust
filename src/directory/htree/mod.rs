mod cursor;
mod node;

use super::{Directory, DirectoryBlock};
use crate::filesystem::FileSystem;
use crate::transaction::{Collector, Transaction};
use crate::{Config, FileBlockNumber, FileType, FsError, InodeNumber, LogicalBlockNumber};
use cursor::HTreeCursor;
use fs_core::DirEntry;
use node::DirDxRoot;
use path::Path;

pub struct HTreeScheme<
    'a,
    C: Config,
    const BLK_SIZE: usize,
    const R_ENTRIES: usize,
    const N_ENTRIES: usize,
    const HAS_TAIL: usize,
> {
    dir: &'a Directory<C, BLK_SIZE>,
}

impl<
        'a,
        C: Config,
        const BLK_SIZE: usize,
        const R_ENTRIES: usize,
        const N_ENTRIES: usize,
        const HAS_TAIL: usize,
    > HTreeScheme<'a, C, BLK_SIZE, R_ENTRIES, N_ENTRIES, HAS_TAIL>
{
    #[inline]
    pub fn new(dir: &'a Directory<C, BLK_SIZE>) -> Self {
        Self { dir }
    }

    #[inline]
    fn get_lba_of_root(&self, fs: &FileSystem<C, BLK_SIZE>) -> Result<LogicalBlockNumber, FsError> {
        dispatch_cursor!(self.dir.inode, fs, FileBlockNumber(0), |mut c| c.current())
            .ok_or(FsError::NoEntry)
            .and_then(|n| {
                n.get_initialized()
                    .ok_or(FsError::InvalidFs("Uninitialized data block in directory"))
            })
    }

    pub fn find_entry(
        &self,
        fs: &FileSystem<C, BLK_SIZE>,
        target: &str,
    ) -> Result<(InodeNumber, Option<FileType>), FsError> {
        let root_lba = self.get_lba_of_root(fs)?;
        let root = fs.blocks.get(root_lba).and_then(|raw| {
            DirDxRoot::<C, BLK_SIZE, R_ENTRIES, N_ENTRIES, HAS_TAIL, false>::from_raw_block(
                raw,
                fs,
                &self.dir.inode,
            )
        })?;
        root.find_then(&self.dir.inode, fs, target, |_, entry| {
            (
                entry.get_inode(),
                entry.get_file_type().map(|n| n.into_file_type()),
            )
        })?
        .ok_or(FsError::NoEntry)
    }

    pub fn has_child(&self, fs: &FileSystem<C, BLK_SIZE>) -> Result<bool, FsError> {
        let root_lba = self.get_lba_of_root(fs)?;
        let root = fs.blocks.get(root_lba).and_then(|raw| {
            DirDxRoot::<C, BLK_SIZE, R_ENTRIES, N_ENTRIES, HAS_TAIL, false>::from_raw_block(
                raw,
                fs,
                &self.dir.inode,
            )
        })?;
        let cursor = HTreeCursor::from_index(&root, 0).unwrap();
        for dir_blk_en in cursor {
            let fba = dir_blk_en.block;
            if let Some(lba) = dispatch_cursor!(self.dir.inode, fs, fba, |mut c| c.current())
                .and_then(|n| n.get_initialized())
            {
                let blk = fs.blocks.get(lba)?;
                let guard = blk.read();
                let dir_blk = DirectoryBlock::new(&**guard, fs);
                for en in dir_blk.iter() {
                    let en = en?;
                    if en.get_inode().0 != 0 {
                        return Ok(true);
                    }
                }
            }
        }
        Ok(false)
    }

    pub fn read_dir(
        &self,
        fs: &FileSystem<C, BLK_SIZE>,
        pos: usize,
    ) -> Result<Option<(DirEntry, usize)>, FsError> {
        let root_lba = self.get_lba_of_root(fs)?;
        let root = fs.blocks.get(root_lba).and_then(|raw| {
            DirDxRoot::<C, BLK_SIZE, R_ENTRIES, N_ENTRIES, HAS_TAIL, false>::from_raw_block(
                raw,
                fs,
                &self.dir.inode,
            )
        })?;
        if pos < 12 {
            Ok(Some((
                DirEntry {
                    path: Path::new(".").to_path_buf(),
                    ino: fs_core::InodeNumber(root.dot().inode.0 as u64),
                    ty: FileType::Directory,
                },
                12,
            )))
        } else if pos < BLK_SIZE {
            Ok(Some((
                DirEntry {
                    path: Path::new("..").to_path_buf(),
                    ino: fs_core::InodeNumber(root.dotdot().inode.0 as u64),
                    ty: FileType::Directory,
                },
                BLK_SIZE,
            )))
        } else {
            let (index, offset) = (pos / BLK_SIZE, pos % BLK_SIZE);

            let fba = if let Some(en) =
                HTreeCursor::from_index(&root, index - 1).and_then(|mut cursor| cursor.next())
            {
                en.block
            } else {
                return Ok(None);
            };
            if let Some(lba) =
                dispatch_cursor!(self.dir.inode, fs, fba, |mut c| c.current()).map(|n| {
                    n.get_initialized()
                        .ok_or(FsError::InvalidFs("Uninitialized data block in directory"))
                })
            {
                let blk = fs.blocks.get(lba?)?;
                let guard = blk.read();
                let dblk = DirectoryBlock::new(&**guard, fs);

                let mut iter = dblk.iter();
                while let Some(en) = iter.next() {
                    if iter.pos() > offset {
                        if let Ok(en) = en {
                            return Ok(Some((
                                DirEntry {
                                    path: Path::new(en.get_name()).to_path_buf(),
                                    ino: fs_core::InodeNumber(en.get_inode().0 as u64),
                                    ty: en
                                        .get_file_type()
                                        .map(|n| n.into_file_type())
                                        .unwrap_or(FileType::Unknown),
                                },
                                iter.pos() + index * BLK_SIZE,
                            )));
                        }
                    }
                }
            }
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
        if let Some(root_block_addr) =
            dispatch_cursor!(self.dir.inode, fs, FileBlockNumber(0), |mut c| c.current()).map(|n| {
                n.get_initialized()
                    .ok_or(FsError::InvalidFs("Uninitialized data block in directory"))
            })
        {
            let raw = fs.blocks.get_mut(root_block_addr?, &tx.collector)?;
            DirDxRoot::<C, BLK_SIZE, R_ENTRIES, N_ENTRIES, HAS_TAIL, true>::from_raw_block(
                raw,
                fs,
                &self.dir.inode,
            )
        } else {
            DirDxRoot::<C, BLK_SIZE, R_ENTRIES, N_ENTRIES, HAS_TAIL, true>::init(
                fs,
                &self.dir.inode,
                tx,
            )
        }
        .and_then(|mut root| root.insert(&self.dir.inode, fs, entry, de, child, tx))
    }

    pub fn remove_entry(
        &self,
        fs: &FileSystem<C, BLK_SIZE>,
        entry: &str,
        collector: &Collector,
    ) -> Result<InodeNumber, FsError> {
        let root_lba = self.get_lba_of_root(fs)?;
        let root = fs.blocks.get_mut(root_lba, collector).and_then(|raw| {
            DirDxRoot::<C, BLK_SIZE, R_ENTRIES, N_ENTRIES, HAS_TAIL, true>::from_raw_block(
                raw,
                fs,
                &self.dir.inode,
            )
        })?;
        root.find_mut_then(&self.dir.inode, fs, entry, collector, |en| en.destroy())?
            .ok_or(FsError::NoEntry)
    }
}
