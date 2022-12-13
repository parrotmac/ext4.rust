// Rework
mod cursor;
mod path;

use crate::block::BlockRef;
use crate::filesystem::FileSystem;
use crate::transaction::Transaction;
use crate::utils::{merge_u32, split_u64, ByteRw};
use crate::{Config, FileBlockNumber, FsError, InodeNumber, LogicalBlockNumber};

use self::path::Path;
pub(crate) use cursor::{Cursor, CursorMut};

#[derive(Clone, Debug)]
pub(crate) struct Internal {
    pub block: FileBlockNumber,
    pub next_node: LogicalBlockNumber,
}

#[derive(Clone, Debug)]
pub(crate) struct Leaf {
    pub block: FileBlockNumber,
    pub start: LogicalBlockNumber,
    pub len: u16,
    pub is_init: bool,
}

impl Leaf {
    #[inline]
    fn contains(&self, block: FileBlockNumber) -> bool {
        self.block <= block && block < self.block + (self.len as u32)
    }
    #[inline]
    pub fn last_fba(&self) -> FileBlockNumber {
        self.block + self.len as u32 - 1
    }
}

#[derive(Clone, Debug)]
pub(crate) enum Entry {
    Internal(Internal),
    Leaf(Leaf),
}

impl Entry {
    #[inline]
    fn first_block(&self) -> FileBlockNumber {
        match self {
            Self::Internal(Internal { block, .. }) | Self::Leaf(Leaf { block, .. }) => *block,
        }
    }

    #[inline]
    fn next_node(&self) -> Option<LogicalBlockNumber> {
        match self {
            Self::Internal(Internal { next_node, .. }) => Some(*next_node),
            _ => None,
        }
    }

    #[inline]
    fn get_internal(self) -> Option<Internal> {
        if let Self::Internal(i) = self {
            Some(i)
        } else {
            None
        }
    }

    #[inline]
    fn get_leaf(self) -> Option<Leaf> {
        if let Self::Leaf(l) = self {
            Some(l)
        } else {
            None
        }
    }
}

fn replace_at<GetRw, T>(
    entries_cnt: u16,
    depth: u16,
    ext: Entry,
    at: usize,
    get_rw: GetRw,
) -> Result<(), Entry>
where
    GetRw: FnOnce() -> ByteRw<T>,
    T: core::convert::AsRef<[u8]>,
    T: core::convert::AsMut<[u8]>,
{
    if at >= entries_cnt as usize {
        return Err(ext);
    }

    let mut rw = get_rw();

    match ext {
        Entry::Leaf(Leaf {
            block,
            start,
            len,
            is_init,
        }) if depth == 0 => {
            let (hi, lo) = split_u64(start.0);
            rw.write_u32(12 * at, block.0);
            rw.write_u16(12 * at + 6, hi.try_into().unwrap());
            rw.write_u32(12 * at + 8, lo);
            rw.write_u16(12 * at + 4, if is_init { len } else { len | 0x8000 });
        }
        Entry::Internal(Internal { block, next_node }) if depth > 0 => {
            let (hi, lo) = split_u64(next_node.0);
            rw.write_u32(12 * at, block.0);
            rw.write_u32(12 * at + 4, lo);
            rw.write_u32(12 * at + 8, hi);
        }
        _ => return Err(ext),
    }
    Ok(())
}

fn insert_at<GetRw, T>(
    max_entries_cnt: u16,
    entries_cnt: u16,
    depth: u16,
    ext: Entry,
    at: usize,
    get_rw: GetRw,
) -> Result<(), Entry>
where
    GetRw: FnOnce() -> ByteRw<T>,
    T: core::convert::AsRef<[u8]>,
    T: core::convert::AsMut<[u8]>,
{
    if max_entries_cnt as usize <= at {
        return Err(ext);
    }

    let mut rw = get_rw();
    if at < entries_cnt as usize {
        // p -> p + 1
        for i in (at + 1..entries_cnt as usize).rev() {
            rw.write_u32(12 * i, rw.read_u32(12 * i - 12));
            rw.write_u32(12 * i + 4, rw.read_u32(12 * i - 12 + 4));
            rw.write_u32(12 * i + 8, rw.read_u32(12 * i - 12 + 8));
        }
    }

    match ext {
        Entry::Leaf(Leaf {
            block,
            start,
            len,
            is_init,
        }) if depth == 0 => {
            let (hi, lo) = split_u64(start.0);
            rw.write_u32(12 * at, block.0);
            rw.write_u16(12 * at + 6, hi.try_into().unwrap());
            rw.write_u32(12 * at + 8, lo);
            rw.write_u16(12 * at + 4, if is_init { len } else { len | 0x8000 });
        }
        Entry::Internal(Internal { block, next_node }) if depth > 0 => {
            let (hi, lo) = split_u64(next_node.0);
            rw.write_u32(12 * at, block.0);
            rw.write_u32(12 * at + 4, lo);
            rw.write_u32(12 * at + 8, hi);
        }
        _ => return Err(ext),
    }
    Ok(())
}

pub struct ExtentTree<C: Config> {
    pub(super) entries_cnt: u16,
    pub(super) max_entries_cnt: u16,
    pub(super) depth: u16,
    pub(super) b: [u8; 48],
    #[cfg(feature = "extent_cache")]
    pub(super) cache: crate::RwLock<Option<Leaf>, C::D>,
    #[cfg(not(feature = "extent_cache"))]
    pub(super) _l: core::marker::PhantomData<C::D>,
}

impl<C: Config> core::fmt::Debug for ExtentTree<C> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("ExtentTree")
            .field("entries", &self.entries_cnt)
            .field("max_entries_cnt", &self.max_entries_cnt)
            .field("depth", &self.depth)
            .field(
                "ext1",
                &[self.get(0), self.get(1), self.get(2), self.get(3)],
            )
            .finish()
    }
}

impl<C: Config> ExtentTree<C> {
    pub(crate) fn cursor_last_mut<'a, 'b, 'c, const BLK_SIZE: usize>(
        &'a mut self,
        ino: InodeNumber,
        fs: &'b FileSystem<C, BLK_SIZE>,
        tx: &'c Transaction,
    ) -> Result<CursorMut<'a, 'b, 'c, C, BLK_SIZE>, FsError>
    where
        'b: 'c,
    {
        let (path, fba) = {
            let mut path: Path<&mut Self, C, BLK_SIZE, true> = Path::empty(ino, self);
            let fba = path.load_last(tx, fs)?;
            (path, fba)
        };
        if let Some(fba) = fba {
            let mut cursor = CursorMut {
                fba,
                ino,
                fs,
                path,
                tx,
                _error: None,
            };
            cursor.move_next()?;
            Ok(cursor)
        } else {
            Ok(CursorMut {
                fba: FileBlockNumber(0),
                ino,
                fs,
                path,
                tx,
                _error: None,
            })
        }
    }

    pub(crate) fn cursor_from_fba<'a, 'b, 'c, const BLK_SIZE: usize>(
        &'a self,
        ino: InodeNumber,
        fs: &'b FileSystem<C, BLK_SIZE>,
        fba: FileBlockNumber,
    ) -> Result<Cursor<'a, 'b, 'c, C, BLK_SIZE>, FsError> {
        #[cfg(feature = "extent_cache")]
        let path = {
            let last = self.cache.try_read().ok().and_then(|n| n.as_ref().cloned());

            let mut path: Path<&Self, C, BLK_SIZE, false> = Path::empty(ino, self);
            match last {
                Some(l) if l.contains(fba) => {
                    path.cache = Some(l);
                }
                _ => {
                    path.load(fba, fs)?;
                }
            };
            path
        };
        #[cfg(not(feature = "extent_cache"))]
        let path = {
            let mut path: Path<&Self, C, BLK_SIZE, false> = Path::empty(ino, self);
            path.load(fba, fs)?;
            path
        };
        Ok(Cursor {
            fba,
            fs,
            path,
            _error: None,
        })
    }

    pub(crate) fn cursor_from_fba_mut<'a, 'b, 'c, const BLK_SIZE: usize>(
        &'a mut self,
        ino: InodeNumber,
        fs: &'b FileSystem<C, BLK_SIZE>,
        fba: FileBlockNumber,
        tx: &'c Transaction,
    ) -> Result<CursorMut<'a, 'b, 'c, C, BLK_SIZE>, FsError>
    where
        'b: 'c,
    {
        #[cfg(feature = "extent_cache")]
        let path = {
            let last = self.cache.try_read().ok().and_then(|n| n.as_ref().cloned());
            let mut path: Path<&mut Self, C, BLK_SIZE, true> = Path::empty(ino, self);
            match last {
                Some(l) if l.contains(fba) => {
                    path.cache = Some(l);
                }
                _ => {
                    path.load(fba, tx, fs)?;
                }
            };
            path
        };
        #[cfg(not(feature = "extent_cache"))]
        let path = {
            let mut path: Path<&mut Self, C, BLK_SIZE, true> = Path::empty(ino, self);
            path.load(fba, tx, fs)?;
            path
        };
        Ok(CursorMut {
            fba,
            ino,
            path,
            fs,
            tx,
            _error: None,
        })
    }
}

impl<C: Config> Default for ExtentTree<C> {
    fn default() -> Self {
        Self {
            entries_cnt: 0,
            max_entries_cnt: 4,
            depth: 0,
            b: [0; 48],
            #[cfg(feature = "extent_cache")]
            cache: crate::RwLock::new(None),
            #[cfg(not(feature = "extent_cache"))]
            _l: core::marker::PhantomData,
        }
    }
}

pub struct Node<'a, 'b, C: Config, const BLK_SIZE: usize, const MUT: bool> {
    b: BlockRef<'a, 'b, C, BLK_SIZE, MUT>,
}

impl<'a, 'b, C: Config, const BLK_SIZE: usize, const MUT: bool> core::fmt::Debug
    for Node<'a, 'b, C, BLK_SIZE, MUT>
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "Node")
    }
}

impl<'a, 'b, C: Config, const BLK_SIZE: usize, const MUT: bool> Node<'a, 'b, C, BLK_SIZE, MUT> {
    pub(crate) fn from_bytes(b: BlockRef<'a, 'b, C, BLK_SIZE, MUT>) -> Result<Self, FsError> {
        let magic = {
            let guard = b.read();
            ByteRw::new(guard.as_ref()).read_u16(0)
        };

        // TODO: checksum.
        if magic == 0xF30A {
            Ok(Self { b })
        } else {
            Err(FsError::InvalidFs("Unexpected extent tree header magic"))
        }
    }

    #[inline]
    fn into_inner(self) -> BlockRef<'a, 'b, C, BLK_SIZE, MUT> {
        self.b
    }
}

trait ExtentHeader {
    fn get_entries_cnt(&self) -> u16;
    fn get_max_entries_cnt(&self) -> u16;
    fn get_depth(&self) -> u16;
    fn get(&self, index: usize) -> Option<Entry>;

    #[inline]
    fn is_full(&self) -> bool {
        self.get_entries_cnt() == self.get_max_entries_cnt()
    }

    #[inline]
    fn binary_search(&self, fba: FileBlockNumber) -> Option<usize> {
        if self.get_entries_cnt() > 0 {
            let (mut l, mut r) = (0, self.get_entries_cnt() - 1);
            while l <= r {
                let m = l + (r - l) / 2;
                if fba < self.get(m as usize).unwrap().first_block() {
                    r = m - 1;
                } else {
                    l = m + 1;
                }
            }
            Some(l as usize - 1)
        } else {
            None
        }
    }
}

impl<C: Config> ExtentHeader for ExtentTree<C> {
    #[inline]
    fn get_entries_cnt(&self) -> u16 {
        self.entries_cnt
    }
    #[inline]
    fn get_max_entries_cnt(&self) -> u16 {
        self.max_entries_cnt
    }
    #[inline]
    fn get_depth(&self) -> u16 {
        self.depth
    }
    #[inline]
    fn get(&self, index: usize) -> Option<Entry> {
        let rw = ByteRw::new(self.b.as_ref());

        if index < self.entries_cnt as usize {
            if self.depth == 0 {
                let (len, is_init) = {
                    let v = rw.read_u16(12 * index + 4);
                    (v & 0x7fff, v < 0x8000)
                };
                Some(Entry::Leaf(Leaf {
                    block: FileBlockNumber(rw.read_u32(12 * index)),
                    start: LogicalBlockNumber(merge_u32(
                        rw.read_u16(12 * index + 6) as u32,
                        rw.read_u32(12 * index + 8),
                    )),
                    len,
                    is_init,
                }))
            } else {
                Some(Entry::Internal(Internal {
                    block: FileBlockNumber(rw.read_u32(12 * index)),
                    next_node: LogicalBlockNumber(merge_u32(
                        rw.read_u16(12 * index + 8) as u32,
                        rw.read_u32(12 * index + 4),
                    )),
                }))
            }
        } else {
            None
        }
    }
}

impl<'a, 'b, C: Config, const BLK_SIZE: usize, const MUT: bool> ExtentHeader
    for Node<'a, 'b, C, BLK_SIZE, MUT>
{
    #[inline]
    fn get_entries_cnt(&self) -> u16 {
        let guard = self.b.read();
        ByteRw::new(guard.as_ref()).read_u16(2)
    }
    #[inline]
    fn get_max_entries_cnt(&self) -> u16 {
        let guard = self.b.read();
        ByteRw::new(guard.as_ref()).read_u16(4)
    }
    #[inline]
    fn get_depth(&self) -> u16 {
        let guard = self.b.read();
        ByteRw::new(guard.as_ref()).read_u16(6)
    }
    #[inline]
    fn get(&self, index: usize) -> Option<Entry> {
        let guard = self.b.read();
        let rw = ByteRw::new(guard.as_ref());

        if index < self.get_entries_cnt() as usize {
            if self.get_depth() == 0 {
                let (len, is_init) = {
                    let v = rw.read_u16(12 + 12 * index + 4);
                    (v & 0x7fff, v <= 0x8000)
                };
                Some(Entry::Leaf(Leaf {
                    block: FileBlockNumber(rw.read_u32(12 + 12 * index)),
                    start: LogicalBlockNumber(merge_u32(
                        rw.read_u16(12 + 12 * index + 6) as u32,
                        rw.read_u32(12 + 12 * index + 8),
                    )),
                    len,
                    is_init,
                }))
            } else {
                Some(Entry::Internal(Internal {
                    block: FileBlockNumber(rw.read_u32(12 + 12 * index)),
                    next_node: LogicalBlockNumber(merge_u32(
                        rw.read_u16(12 + 12 * index + 8) as u32,
                        rw.read_u32(12 + 12 * index + 4),
                    )),
                }))
            }
        } else {
            None
        }
    }
}

trait ExtentHeaderMut
where
    Self: ExtentHeader,
{
    fn set_entries_cnt(&mut self, v: u16);
    fn set_max_entries_cnt(&mut self, v: u16);
    fn set_depth(&mut self, v: u16);
}

impl<C: Config> ExtentHeaderMut for ExtentTree<C> {
    #[inline]
    fn set_entries_cnt(&mut self, v: u16) {
        self.entries_cnt = v;
    }
    #[inline]
    fn set_max_entries_cnt(&mut self, v: u16) {
        self.max_entries_cnt = v;
    }
    #[inline]
    fn set_depth(&mut self, v: u16) {
        self.depth = v;
    }
}

impl<'a, 'b, C: Config, const BLK_SIZE: usize> ExtentHeaderMut for Node<'a, 'b, C, BLK_SIZE, true> {
    #[inline]
    fn set_entries_cnt(&mut self, v: u16) {
        let mut guard = self.b.write();
        ByteRw::new(guard.as_mut()).write_u16(2, v);
    }
    #[inline]
    fn set_max_entries_cnt(&mut self, v: u16) {
        let mut guard = self.b.write();
        ByteRw::new(guard.as_mut()).write_u16(4, v);
    }
    #[inline]
    fn set_depth(&mut self, v: u16) {
        let mut guard = self.b.write();
        ByteRw::new(guard.as_mut()).write_u16(6, v);
    }
}

impl<C: Config> ExtentTree<C> {
    #[inline]
    fn insert_at(&mut self, ext: Entry, at: usize) -> Result<(), Entry> {
        let (max_entries_cnt, entries_cnt, depth) = (
            self.get_max_entries_cnt(),
            self.get_entries_cnt(),
            self.get_depth(),
        );
        insert_at(max_entries_cnt, entries_cnt, depth, ext, at, || {
            ByteRw::new(&mut self.b)
        })
        .map(|_| self.set_entries_cnt(self.get_entries_cnt() + 1))
    }
    #[inline]
    fn replace_at(&mut self, ext: Entry, at: usize) -> Result<(), Entry> {
        let (entries_cnt, depth) = (self.get_entries_cnt(), self.get_depth());

        replace_at(entries_cnt, depth, ext, at, || ByteRw::new(&mut self.b))
    }
}

impl<'a, 'b, C: Config, const BLK_SIZE: usize> Node<'a, 'b, C, BLK_SIZE, true> {
    #[inline]
    fn insert_at(&mut self, ext: Entry, at: usize) -> Result<(), Entry> {
        let (max_entries_cnt, entries_cnt, depth) = (
            self.get_max_entries_cnt(),
            self.get_entries_cnt(),
            self.get_depth(),
        );
        {
            let mut guard = self.b.write();

            insert_at(max_entries_cnt, entries_cnt, depth, ext, at, || {
                ByteRw::new(&mut guard[12..])
            })
        }
        .map(|_| self.set_entries_cnt(self.get_entries_cnt() + 1))
    }
    #[inline]
    fn replace_at(&mut self, ext: Entry, at: usize) -> Result<(), Entry> {
        let (entries_cnt, depth) = (self.get_entries_cnt(), self.get_depth());
        let mut guard = self.b.write();

        replace_at(entries_cnt, depth, ext, at, || {
            ByteRw::new(&mut guard[12..])
        })
    }
}
