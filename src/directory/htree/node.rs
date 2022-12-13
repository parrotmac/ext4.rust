use super::cursor::HTreeCursor;
use crate::block::BlockRef;
use crate::crc::Crc32c;
use crate::directory::block::DirectoryBlockIteratorMutCursor;
use crate::directory::entry::DirectoryEntryDispatch;
use crate::directory::DirectoryBlock;
use crate::filesystem::FileSystem;
use crate::hasher::{Ext4Hasher, HashVersion};
use crate::inode::Ext4De;
use crate::inode::Inode;
use crate::superblock::Ext4Flag;
use crate::transaction::{Collector, Transaction};
use crate::utils::ByteRw;
use crate::{Config, FileBlockNumber, FileType, FsError, InodeNumber};
use alloc::collections::BTreeMap;
use core::convert::TryFrom;
use core::hash::Hasher;

#[repr(C)]
#[derive(Debug)]
pub struct DirDxDotEn {
    pub(crate) inode: InodeNumber,
    pub(crate) rec_len: u16,
    pub(crate) name_len: u8,
    pub(crate) file_type: u8,
    pub(crate) name: [u8; 4],
}

#[derive(Debug)]
pub struct RootInfo {
    pub(crate) info_length: u8,
    pub(crate) indirect_levels: u8,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct DxEntry {
    pub hash: u32,
    pub block: FileBlockNumber,
}

#[repr(C)]
#[derive(Debug)]
pub struct CLimit {
    pub limit: u16,
    pub count: u16,
    pub block: FileBlockNumber,
}

pub struct DirDxNode<
    'l,
    'j,
    C: Config,
    const BLK_SIZE: usize,
    const ENTRIES: usize,
    const HAS_TAIL: usize,
    const MUT: bool,
> {
    _raw: BlockRef<'l, 'j, C, BLK_SIZE, MUT>,
}

impl<
        'l,
        'j,
        C: Config,
        const BLK_SIZE: usize,
        const ENTRIES: usize,
        const HAS_TAIL: usize,
        const MUT: bool,
    > DirDxNode<'l, 'j, C, BLK_SIZE, ENTRIES, HAS_TAIL, MUT>
{
    #[inline]
    fn _fake_len(&self) -> u16 {
        let guard = self._raw.read();
        let reader = ByteRw::new(guard.as_ref());
        reader.read_u16(4)
    }

    #[inline]
    fn _climit(&self) -> CLimit {
        let guard = self._raw.read();
        let reader = ByteRw::new(guard.as_ref());
        CLimit {
            limit: reader.read_u16(0x8),
            count: reader.read_u16(0xA),
            block: FileBlockNumber(reader.read_u32(0xE)),
        }
    }

    #[inline]
    fn _verify_csum(
        &self,
        fs: &FileSystem<C, BLK_SIZE>,
        inode: &Inode<C, BLK_SIZE>,
    ) -> Result<(), FsError> {
        if HAS_TAIL != 0 {
            let CLimit { count, limit, .. } = self._climit();
            let b = self._raw.read();

            let size = 0x12 + count as usize * core::mem::size_of::<DxEntry>();
            let tail = 0x12 + limit as usize * core::mem::size_of::<DxEntry>();
            let ino: u32 = inode.ino.0;

            let mut hasher = Crc32c::default();
            hasher.write(&fs.sb.uuid);
            hasher.write(&ino.to_le_bytes());
            hasher.write(&inode.generation.to_le_bytes());
            hasher.write(&b[..size]);
            hasher.write(&b[tail..tail + 4]);
            hasher.write(&0_i32.to_le_bytes());

            if hasher.finish() as u32 == ByteRw::new(b.as_ref()).read_u32(tail + 4) {
                Ok(())
            } else {
                Err(FsError::InvalidFs("Failed to verify HTree checksum."))
            }
        } else {
            Ok(())
        }
    }

    fn _verify(
        self,
        fs: &FileSystem<C, BLK_SIZE>,
        inode: &Inode<C, BLK_SIZE>,
    ) -> Result<Self, FsError> {
        if self._climit().limit as usize == ENTRIES + 1 && self._fake_len() == BLK_SIZE as u16 {
            self._verify_csum(fs, inode).map(|_| self)
        } else {
            Err(FsError::InvalidFs("HTree climit is corrupted."))
        }
    }
}

pub struct DirDxRoot<
    'l,
    'j,
    C: Config,
    const BLK_SIZE: usize,
    const ENTRIES: usize,
    const N_ENTRIES: usize,
    const HAS_TAIL: usize,
    const MUT: bool,
> {
    raw: BlockRef<'l, 'j, C, BLK_SIZE, MUT>,
    hasher: Ext4Hasher,
}

impl<
        'l,
        'j,
        C: Config,
        const BLK_SIZE: usize,
        const ENTRIES: usize,
        const N_ENTRIES: usize,
        const HAS_TAIL: usize,
        const IS_MUT: bool,
    > DirDxRoot<'l, 'j, C, BLK_SIZE, ENTRIES, N_ENTRIES, HAS_TAIL, IS_MUT>
{
    #[inline]
    pub(crate) fn dot(&self) -> DirDxDotEn {
        let guard = self.raw.read();
        let reader = ByteRw::new(guard.as_ref());
        DirDxDotEn {
            inode: InodeNumber(reader.read_u32(0)),
            rec_len: reader.read_u16(4),
            name_len: reader.read_u8(6),
            file_type: reader.read_u8(7),
            name: [
                reader.read_u8(8),
                reader.read_u8(9),
                reader.read_u8(10),
                reader.read_u8(11),
            ],
        }
    }

    #[inline]
    pub(crate) fn dotdot(&self) -> DirDxDotEn {
        let guard = self.raw.read();
        let reader = ByteRw::new(guard.as_ref());
        DirDxDotEn {
            inode: InodeNumber(reader.read_u32(0xC)),
            rec_len: reader.read_u16(0x10),
            name_len: reader.read_u8(0x12),
            file_type: reader.read_u8(0x13),
            name: [
                reader.read_u8(0x14),
                reader.read_u8(0x15),
                reader.read_u8(0x16),
                reader.read_u8(0x17),
            ],
        }
    }

    #[inline]
    pub(crate) fn root_info(&self) -> RootInfo {
        let guard = self.raw.read();
        let reader = ByteRw::new(guard.as_ref());
        RootInfo {
            info_length: reader.read_u8(0x1D),
            indirect_levels: reader.read_u8(0x1E),
        }
    }

    #[inline]
    pub(crate) fn climit(&self) -> CLimit {
        let guard = self.raw.read();
        let reader = ByteRw::new(guard.as_ref());
        CLimit {
            limit: reader.read_u16(0x20),
            count: reader.read_u16(0x22),
            block: FileBlockNumber(reader.read_u32(0x24)),
        }
    }

    pub fn entries(&self, index: usize) -> Option<DxEntry> {
        let guard = self.raw.read();
        let climit = self.climit();
        let reader = ByteRw::new(guard.as_ref());
        if index == 0 {
            Some(DxEntry {
                hash: 0,
                block: climit.block,
            })
        } else if index < climit.count as usize {
            Some(DxEntry {
                hash: reader.read_u32(0x28 + 8 * index - 8),
                block: FileBlockNumber(reader.read_u32(0x2c + 8 * index - 8)),
            })
        } else {
            None
        }
    }

    #[inline]
    fn verify_csum(
        &self,
        _fs: &FileSystem<C, BLK_SIZE>,
        _inode: &Inode<C, BLK_SIZE>,
    ) -> Result<(), FsError> {
        /*
        if HAS_TAIL != 0 {
            let CLimit { count, .. } = self.climit::<BLK_SIZE>()?;

            let tail = &self.dx_tail[0];
            let size = core::mem::size_of::<DirDxDotEn>() * 2
                + count.into_native() as usize * core::mem::size_of::<DxEntry>();
            let ino: u32 = inode.ino.0.try_into().unwrap();

            let mut hasher = Crc32c::new(EXT4_CRC32_INIT);
            hasher.write(&sb.uuid);
            hasher.write(&ino.to_le_bytes());
            hasher.write(&inode.generation.to_le_bytes());
            hasher.write(&self.raw.bytes()[..size]);
            hasher.write(&tail._reserved.into_native().to_le_bytes());
            hasher.write(&0_i32.to_le_bytes());

            if hasher.finish() as u32 == tail.csum.into_native() {
                Ok(())
            } else {
                Err(FsError::InvalidFs(ERR_HTREE_CORRUPTED))
            }
        } else {
            Ok(())
        }
        */
        Ok(())
    }

    fn verify(
        self,
        fs: &FileSystem<C, BLK_SIZE>,
        inode: &Inode<C, BLK_SIZE>,
    ) -> Result<Self, FsError> {
        if self.dot().rec_len == 12
            && self.dotdot().rec_len == BLK_SIZE as u16 - 12
            && self.root_info().info_length == 8
            && self.root_info().indirect_levels <= 1
            && self.climit().limit as usize == ENTRIES + 1
        {
            self.verify_csum(fs, inode).map(|_| self)
        } else {
            Err(FsError::InvalidFs("Invalid HTree root detected."))
        }
    }

    pub(crate) fn from_raw_block(
        raw: BlockRef<'l, 'j, C, BLK_SIZE, IS_MUT>,
        fs: &FileSystem<C, BLK_SIZE>,
        inode: &Inode<C, BLK_SIZE>,
    ) -> Result<DirDxRoot<'l, 'j, C, BLK_SIZE, ENTRIES, N_ENTRIES, HAS_TAIL, IS_MUT>, FsError> {
        let hasher = HashVersion::try_from(ByteRw::new(raw.read().as_ref()).read_u8(0x1C))
            .map_err(|_| FsError::Unsupported("Unsupported Hash"))?
            .get_hasher(
                fs.sb.hash_seed,
                fs.sb.flags.contains(Ext4Flag::UNSIGNED_HASH),
            );
        DirDxRoot { raw, hasher }.verify(fs, inode)
    }

    #[inline]
    fn hash_of(&self, target: &str) -> u32 {
        let mut hasher = self.hasher;
        hasher.write(target.as_bytes());
        (hasher.finish() >> 32) as u32
    }

    pub(crate) fn find_then<R>(
        &self,
        inode: &Inode<C, BLK_SIZE>,
        fs: &FileSystem<C, BLK_SIZE>,
        target: &str,
        f: impl FnOnce(Option<DirectoryEntryDispatch<&[u8]>>, DirectoryEntryDispatch<&[u8]>) -> R,
    ) -> Result<Option<R>, FsError> {
        if target == "." || target == ".." {
            let mut prev = None;
            let lba = dispatch_cursor!(inode, fs, FileBlockNumber(0), |mut c| c.current())
                .ok_or(FsError::NoEntry)
                .and_then(|n| {
                    n.get_initialized()
                        .ok_or(FsError::InvalidFs("Uninitialized data block in directory"))
                })?;
            let blk = fs.blocks.get(lba)?;
            for entry in DirectoryBlock::new(&**blk.read(), fs).iter() {
                let entry = entry?;
                if entry.get_name() == target && entry.get_inode().0 != 0 {
                    return Ok(Some(f(prev, entry)));
                }
                prev = Some(entry);
            }
        }

        let hash = self.hash_of(target);
        for en in HTreeCursor::from_hash(self, hash)? {
            let mut prev = None;
            let lba = dispatch_cursor!(inode, fs, en.block, |mut c| c.current())
                .ok_or(FsError::NoEntry)
                .and_then(|n| {
                    n.get_initialized()
                        .ok_or(FsError::InvalidFs("Uninitialized data block in directory"))
                })?;
            let blk = fs.blocks.get(lba)?;
            for entry in DirectoryBlock::new(&**blk.read(), fs).iter() {
                let entry = entry?;
                if entry.get_name() == target && entry.get_inode().0 != 0 {
                    return Ok(Some(f(prev, entry)));
                }
                prev = Some(entry);
            }
        }
        Ok(None)
    }
}

#[derive(Debug)]
struct DirEntry<'a> {
    inode: InodeNumber,
    name: &'a str,
    ty: Option<Ext4De>,
}

impl<
        'l,
        'j,
        C: Config,
        const BLK_SIZE: usize,
        const ENTRIES: usize,
        const N_ENTRIES: usize,
        const HAS_TAIL: usize,
    > DirDxRoot<'l, 'j, C, BLK_SIZE, ENTRIES, N_ENTRIES, HAS_TAIL, true>
{
    pub(crate) fn insert_at(&self, at: usize, entry: DxEntry) -> Result<(), DxEntry> {
        let climit = self.climit();
        let mut guard = self.raw.write();
        let mut rw = ByteRw::new(guard.as_mut());
        if at == 0 {
            unreachable!()
        } else if at >= climit.limit as usize {
            return Err(entry);
        }

        if at < climit.count as usize {
            for i in (at..climit.count as usize).rev() {
                rw.write_u32(0x28 + 8 * i, rw.read_u32(0x28 + 8 * i - 8));
                rw.write_u32(0x2c + 8 * i, rw.read_u32(0x2c + 8 * i - 8));
            }
        }

        let DxEntry { hash, block } = entry;
        rw.write_u32(0x28 + 8 * at - 8, hash);
        rw.write_u32(0x2c + 8 * at - 8, block.0);
        // climit.cnt += 1;
        rw.write_u16(0x22, climit.count + 1);
        Ok(())
    }

    fn split_data<'a>(
        &self,
        inode: &Inode<C, BLK_SIZE>,
        mut cursor: HTreeCursor<'a, 'l, 'j, C, BLK_SIZE, ENTRIES, N_ENTRIES, HAS_TAIL, true>,
        en: DirEntry,
        fs: &FileSystem<C, BLK_SIZE>,
        dx_en: DxEntry,
        tx: &'l Transaction,
    ) -> Result<(), FsError> {
        let mut entries = BTreeMap::new();
        let blk = dispatch_cursor!(inode, fs, dx_en.block, |mut c| {
            let lba = c.current().unwrap().get_initialized().unwrap();
            fs.blocks.get_mut(lba, &tx.collector)?
        });
        let guard = blk.read().clone();
        let dblk = DirectoryBlock::new(guard, fs);

        // Gather and sort the entries in the last block.
        for entry in dblk.iter() {
            let entry = entry?;
            let (inode, ty, name) = (
                entry.get_inode(),
                entry.get_file_type(),
                entry.get_name_for_sort(),
            );
            entries.insert(self.hash_of(name), DirEntry { inode, name, ty });
        }
        entries.insert(self.hash_of(en.name), en);

        // Allocate new block.
        let (dx_fba, new_blk) = dispatch_cursor_last_mut!(inode, fs, tx, |mut c| {
            let lba = c.or_allocated(false).unwrap();
            (c.fba(), fs.blocks.get_mut_noload(lba, &tx.collector)?)
        });

        let size = inode.get_size();
        inode.set_size(size + BLK_SIZE as u64, tx);

        // Find half of entry by size.
        let (mut used, mut phash) = (0, u32::MAX);
        let (new_hash, split_pos) = entries
            .iter()
            .enumerate()
            .find_map(|(idx, (e_hash, en))| {
                used += en.name.len() + 8;
                if used > BLK_SIZE / 2 {
                    // Handle hash collision
                    Some((
                        if phash == *e_hash {
                            e_hash + 1
                        } else {
                            *e_hash
                        },
                        idx,
                    ))
                } else {
                    phash = *e_hash;
                    None
                }
            })
            .unwrap();

        // Insert into the htree.
        cursor.insert(
            DxEntry {
                hash: new_hash,
                block: dx_fba,
            },
            fs,
            tx,
        )?;

        // Put entries into blocks.
        let mut iter = entries.into_values();
        // Put first half of data into old block.
        {
            let mut guard = blk.write();
            let mut dblk = DirectoryBlock::new(&mut **guard, fs);
            dblk.clear();
            for _ in 0..split_pos {
                let DirEntry { inode, name, ty } = iter.next().unwrap();
                dblk.insert_entry(name, ty.map(|de| de.into_file_type()), inode)?;
            }
        }
        // Put remainder into new block.
        {
            let mut guard = new_blk.write();
            let mut dblk = DirectoryBlock::new(&mut **guard, fs);
            dblk.clear();
            for DirEntry { inode, name, ty } in iter {
                dblk.insert_entry(name, ty.map(|de| de.into_file_type()), inode)?;
            }
        }
        Ok(())
    }

    pub fn insert(
        &mut self,
        inode: &Inode<C, BLK_SIZE>,
        fs: &FileSystem<C, BLK_SIZE>,
        entry: &str,
        de: FileType,
        child: InodeNumber,
        tx: &'l Transaction,
    ) -> Result<(), FsError> {
        // ext4_dir_idx.c: 1283
        if entry == "." {
            let mut guard = self.raw.write();
            let mut rw = ByteRw::new(guard.as_mut());
            // dot.inode
            rw.write_u32(0x0, child.0);
            return Ok(());
        } else if entry == ".." {
            let mut guard = self.raw.write();
            let mut rw = ByteRw::new(guard.as_mut());
            rw.write_u32(0xC, child.0);
            return Ok(());
        }

        let hash = self.hash_of(entry);
        let mut cursor = HTreeCursor::from_hash(self, hash)?;
        cursor.try_split()?;

        let mut last_dx_en = None;
        for dx_en in &mut cursor {
            // XXX: it is possible to grow the inode block in this point?
            dispatch_cursor!(inode, fs, dx_en.block, |mut c| {
                let lba = c.current().unwrap().get_initialized().unwrap();
                let blk = fs.blocks.get_mut(lba, &tx.collector)?;
                let mut guard = blk.write();
                let mut dblk = DirectoryBlock::new(&mut **guard, fs);
                if dblk.insert_entry(entry, Some(de), child)? {
                    return Ok(());
                }
            });
            last_dx_en = Some(dx_en);
        }
        cursor.has_error()?;
        cursor.root.0.split_data(
            inode,
            cursor,
            DirEntry {
                name: entry,
                ty: Some(Ext4De::from_file_type(de)),
                inode: child,
            },
            fs,
            last_dx_en.unwrap(),
            tx,
        )
    }

    pub fn find_mut_then<R>(
        &self,
        inode: &Inode<C, BLK_SIZE>,
        fs: &FileSystem<C, BLK_SIZE>,
        target: &str,
        collector: &'l Collector,
        f: impl FnOnce(DirectoryBlockIteratorMutCursor<&mut [u8; BLK_SIZE], BLK_SIZE>) -> R,
    ) -> Result<Option<R>, FsError> {
        if target == "." || target == ".." {
            let lba = dispatch_cursor!(inode, fs, FileBlockNumber(0), |mut c| c.current())
                .ok_or(FsError::InvalidFs(
                    "Fail to index the logical block number during find_mut_then",
                ))
                .and_then(|n| {
                    n.get_initialized()
                        .ok_or(FsError::InvalidFs("Uninitialized data block in directory"))
                })?;
            let blk = fs.blocks.get_mut(lba, collector)?;
            let mut guard = blk.write();
            let mut db = DirectoryBlock::new(&mut **guard, fs);
            let mut iter = db.iter_mut();

            while let Some(entry) = iter.next() {
                let cursor = entry?;
                if cursor.current().get_name() == target && cursor.current().get_inode().0 != 0 {
                    return Ok(Some(f(cursor)));
                }
            }
        }

        let hash = self.hash_of(target);
        let mut cursor = HTreeCursor::from_hash(self, hash)?;
        for en in &mut cursor {
            let lba = dispatch_cursor!(inode, fs, en.block, |mut c| c.current())
                .ok_or(FsError::InvalidFs(
                    "Fail to index the logical block number during find_mut_then",
                ))
                .and_then(|n| {
                    n.get_initialized()
                        .ok_or(FsError::InvalidFs("Uninitialized data block in directory"))
                })?;
            let blk = fs.blocks.get_mut(lba, collector)?;
            let mut guard = blk.write();
            let mut db = DirectoryBlock::new(&mut **guard, fs);
            let mut iter = db.iter_mut();

            while let Some(entry) = iter.next() {
                let cursor = entry?;
                if cursor.current().get_name() == target && cursor.current().get_inode().0 != 0 {
                    return Ok(Some(f(cursor)));
                }
            }
        }
        cursor.has_error()?;
        Ok(None)
    }

    pub(crate) fn init(
        fs: &'l FileSystem<C, BLK_SIZE>,
        inode: &Inode<C, BLK_SIZE>,
        tx: &'j Transaction,
    ) -> Result<DirDxRoot<'l, 'j, C, BLK_SIZE, ENTRIES, N_ENTRIES, HAS_TAIL, true>, FsError>
    where
        'l: 'j,
    {
        let root_block_addr =
            dispatch_cursor_mut!(inode, fs, FileBlockNumber(0), tx, |mut cursor| {
                cursor.or_allocated(true)?
            });
        {
            let b1 = dispatch_cursor_mut!(inode, fs, FileBlockNumber(1), tx, |mut cursor| {
                cursor.or_allocated(true)?
            });
            let blk = fs.blocks.get_mut(b1, &tx.collector)?;
            DirectoryBlock::new(&mut **blk.write(), fs).clear();
        }
        inode.set_size(BLK_SIZE as u64 * 2, tx);
        let raw = fs.blocks.get_mut_noload(root_block_addr, &tx.collector)?;

        {
            let mut guard = raw.write();
            let mut rw = ByteRw::new(guard.as_mut());
            // fill dot
            rw.write_u32(0x0, 0);
            rw.write_u16(0x4, 12);
            rw.write_u8(0x6, 1);
            rw.write_u8(0x7, 2);
            rw.write_u8(0x8, b'.');
            rw.write_u8(0x9, 0);
            rw.write_u8(0xa, 0);
            rw.write_u8(0xb, 0);

            // fill dotdot.
            rw.write_u32(0xC, 0);
            rw.write_u16(0x10, BLK_SIZE as u16 - 12);
            rw.write_u8(0x12, 2);
            rw.write_u8(0x13, 2);
            rw.write_u8(0x14, b'.');
            rw.write_u8(0x15, b'.');
            rw.write_u8(0x16, 0);
            rw.write_u8(0x17, 0);

            // fill root_info
            rw.write_u32(0x18, 0);
            rw.write_u8(0x1c, fs.sb.default_hash_version);
            rw.write_u8(0x1d, 8);
            rw.write_u8(0x1e, 0);
            rw.write_u8(0x1f, 0);

            // fill climit
            rw.write_u16(0x20, ENTRIES as u16 + 1);
            rw.write_u16(0x22, 1);
            rw.write_u32(0x24, 1);

            if HAS_TAIL != 0 {
                todo!()
            }
        }

        Self::from_raw_block(raw, fs, inode)
    }
}
