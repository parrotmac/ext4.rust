use super::{Entry, ExtentHeader, ExtentHeaderMut, ExtentTree, Internal, Leaf, Node};
use crate::block::BlockRef;
use crate::filesystem::FileSystem;
use crate::transaction::Transaction;
use crate::utils::ByteRw;
use crate::{Config, FileBlockNumber, FsError, InodeNumber, LogicalBlockNumber};
use alloc::vec::Vec;

pub(super) struct Path<'a, 'b, T, C: Config, const BLK_SIZE: usize, const MUT: bool>
where
    T: core::ops::Deref<Target = ExtentTree<C>>,
{
    ino: InodeNumber,
    pub(super) root: (T, Option<usize>),
    pub(super) leafs: Vec<(Node<'a, 'b, C, BLK_SIZE, MUT>, Option<usize>)>,
    #[cfg(feature = "extent_cache")]
    pub(super) cache: Option<Leaf>,
}

enum PathEntry<'a, 'b, 'c, T, C: Config, const BLK_SIZE: usize, const MUT: bool>
where
    T: core::ops::Deref<Target = ExtentTree<C>>,
{
    Root(&'b (T, Option<usize>)),
    Leaf(&'b (Node<'a, 'c, C, BLK_SIZE, MUT>, Option<usize>)),
}

pub(super) enum PathEntryMut<'a, 'b, 'c, T, C: Config, const BLK_SIZE: usize, const MUT: bool>
where
    T: core::ops::Deref<Target = ExtentTree<C>>,
{
    Root(&'b mut (T, Option<usize>)),
    Leaf(&'b mut (Node<'a, 'c, C, BLK_SIZE, MUT>, Option<usize>)),
}

macro_rules! dispatch_entry {
    ($en:expr, |$node:ident, $idx:ident| $code:expr) => {
        match $en {
            PathEntry::Root(($node, $idx)) => $code,
            PathEntry::Leaf(($node, $idx)) => $code,
        }
    };
}

macro_rules! dispatch_entry_mut {
    ($en:expr, |$node:ident, $idx:ident| $code:expr) => {
        match $en {
            PathEntryMut::Root(($node, $idx)) => $code,
            PathEntryMut::Leaf(($node, $idx)) => $code,
        }
    };
}

impl<'a, 'b, T, C: Config, const BLK_SIZE: usize, const MUT: bool> Path<'a, 'b, T, C, BLK_SIZE, MUT>
where
    T: core::ops::Deref<Target = ExtentTree<C>>,
{
    #[inline]
    pub fn empty(ino: InodeNumber, t: T) -> Self {
        let leafs = Vec::with_capacity(t.get_depth() as usize);
        Self {
            ino,
            root: (t, None),
            leafs,
            #[cfg(feature = "extent_cache")]
            cache: None,
        }
    }

    #[inline]
    fn len(&self) -> usize {
        1 + self.leafs.len()
    }

    #[inline]
    fn get<'c>(&'c self, at: usize) -> Option<PathEntry<'a, 'c, 'b, T, C, BLK_SIZE, MUT>> {
        if at == 0 {
            Some(PathEntry::Root(&self.root))
        } else {
            self.leafs.get(at - 1).map(PathEntry::Leaf)
        }
    }

    #[inline]
    fn get_mut<'c>(
        &'c mut self,
        at: usize,
    ) -> Option<PathEntryMut<'a, 'c, 'b, T, C, BLK_SIZE, MUT>> {
        if at == 0 {
            Some(PathEntryMut::Root(&mut self.root))
        } else {
            self.leafs.get_mut(at - 1).map(PathEntryMut::Leaf)
        }
    }

    #[inline]
    fn last<'c>(&'c self) -> PathEntry<'a, 'c, 'b, T, C, BLK_SIZE, MUT> {
        self.get(self.len() - 1).unwrap()
    }

    #[inline]
    pub(super) fn last_mut<'c>(&'c mut self) -> PathEntryMut<'a, 'c, 'b, T, C, BLK_SIZE, MUT> {
        self.get_mut(self.len() - 1).unwrap()
    }

    #[inline]
    pub(super) fn get_leaf(&self) -> Option<Leaf> {
        #[cfg(feature = "extent_cache")]
        let c = self.cache.clone();
        #[cfg(not(feature = "extent_cache"))]
        let c = None;
        let en = c.map(Entry::Leaf).or_else(|| {
            if self.leafs.is_empty() {
                let (root_node, idx) = &self.root;
                idx.and_then(|n| root_node.get(n))
            } else {
                let (node, idx) = self.leafs.last().unwrap();
                idx.and_then(|n| node.get(n))
            }
        });
        match en {
            Some(Entry::Leaf(l)) => Some(l),
            _ => None,
        }
    }
}

impl<'a, 'b, T, C: Config, const BLK_SIZE: usize> Path<'a, 'b, T, C, BLK_SIZE, false>
where
    T: core::ops::Deref<Target = ExtentTree<C>>,
{
    pub fn load(
        &mut self,
        fba: FileBlockNumber,
        fs: &'a FileSystem<C, BLK_SIZE>,
    ) -> Result<(), FsError> {
        let Self { root, leafs, .. } = self;
        leafs.clear();

        let root_index = root.0.binary_search(fba);
        root.1 = root_index;
        let mut last_node = root_index.and_then(|idx| root.0.get(idx));

        for _ in (0..root.0.get_depth()).rev() {
            if let Some(ext) = last_node.take() {
                let node = fs
                    .blocks
                    .get(ext.next_node().unwrap())
                    .and_then(Node::from_bytes)?;
                let index = node.binary_search(fba);
                last_node = index.and_then(|idx| node.get(idx));
                leafs.push((node, index))
            } else {
                break;
            }
        }
        Ok(())
    }
}

impl<'a, 'b, C: Config, const BLK_SIZE: usize> Path<'a, 'b, &mut ExtentTree<C>, C, BLK_SIZE, true>
where
    'a: 'b,
{
    /*
    fn move_back<IO, S>(
        &mut self,
        tx: &Transaction,
        fs: &'a FileSystemInner<C, BLK_SIZE>,
    ) -> Result<(), FsError>
    where
        IO: IoEndPoint,
        S: Dreamer,
    {
        let Self { root, leafs } = self;
        leafs.clear();

        let root_index = match root.0.get_entries_cnt() {
            0 => None,
            cnt => Some(cnt as usize - 1),
        };
        let mut last_node = root_index.and_then(|idx| root.0.get(idx));

        for _ in (0..root.0.get_depth()).rev() {
            if let Some(ext) = last_node.take() {
                let node = fs
                    .blocks
                    .get_mut(ext.next_node().unwrap(), &tx.collector)
                    .and_then(Node::from_bytes)?;
                let index = match node.get_entries_cnt() {
                    0 => None,
                    cnt => Some(cnt as usize - 1),
                };
                last_node = index.and_then(|idx| node.get(idx));
                leafs.push((node, index))
            } else {
                break;
            }
        }
        Ok(())
    }
    */

    pub fn load(
        &mut self,
        fba: FileBlockNumber,
        tx: &'b Transaction,
        fs: &'a FileSystem<C, BLK_SIZE>,
    ) -> Result<(), FsError> {
        let Self { root, leafs, .. } = self;
        leafs.clear();

        let root_index = root.0.binary_search(fba);
        root.1 = root_index;

        let mut last_node = root_index.and_then(|idx| root.0.get(idx));
        for _ in (0..root.0.get_depth()).rev() {
            if let Some(ext) = last_node.take() {
                let node = fs
                    .blocks
                    .get_mut(ext.next_node().unwrap(), &tx.collector)
                    .and_then(Node::from_bytes)?;
                let index = node.binary_search(fba);
                last_node = index.and_then(|idx| node.get(idx));
                leafs.push((node, index))
            } else {
                break;
            }
        }
        Ok(())
    }

    pub fn load_last(
        &mut self,
        tx: &'b Transaction,
        fs: &'a FileSystem<C, BLK_SIZE>,
    ) -> Result<Option<FileBlockNumber>, FsError> {
        let Self { root, leafs, .. } = self;
        leafs.clear();

        if let Some(root_index) = root.0.get_entries_cnt().checked_sub(1) {
            root.1 = Some(root_index as usize);

            let mut last_node = root.0.get(root_index as usize);

            for _ in (0..root.0.get_depth()).rev() {
                if let Some(ext) = last_node.take() {
                    let node = fs
                        .blocks
                        .get_mut(ext.next_node().unwrap(), &tx.collector)
                        .and_then(Node::from_bytes)?;
                    let index = Some(node.get_entries_cnt() as usize - 1);
                    last_node = index.and_then(|idx| node.get(idx));
                    leafs.push((node, index))
                } else {
                    break;
                }
            }
            Ok(Some(
                last_node
                    .unwrap_or_else(|| root.0.get(root_index as usize).unwrap())
                    .get_leaf()
                    .unwrap()
                    .last_fba(),
            ))
        } else {
            root.1 = None;
            Ok(None)
        }
    }

    fn insert_leaf(&mut self, new: Leaf) -> Result<Option<Leaf>, FsError> {
        let leaf = self.get_leaf();

        if leaf.as_ref().map(|l| l.block == new.block).unwrap_or(false) {
            return Err(FsError::InvalidFs("Extent tree is corrupted"));
        }

        dispatch_entry_mut!(self.last_mut(), |node, index| {
            if let Some(Entry::Leaf(Leaf {
                block,
                start,
                len,
                is_init,
            })) = index.and_then(|i| node.get(i))
            {
                if block + len as u32 == new.block
                    && start + (len as u64) == new.start
                    && len + new.len <= 0x8000
                    && is_init
                {
                    node.replace_at(
                        Entry::Leaf(Leaf {
                            block,
                            start,
                            len: len + new.len,
                            is_init,
                        }),
                        index.unwrap(),
                    )
                    .unwrap_or_else(|_| unreachable!());
                    return Ok(None);
                }
            }

            if node.is_full() {
                return Ok(Some(new));
            }

            if leaf.is_none() {
                *index = Some(0);
            } else if new.block > leaf.unwrap().block {
                *index = Some(index.unwrap() + 1);
            }

            let mut pos = index.unwrap();
            let updated_start = new.block;
            if let Err(_en) = node.insert_at(Entry::Leaf(new), pos) {
                todo!()
            }

            for idx in (0..self.len() - 1).rev() {
                if pos != 0 {
                    break;
                }
                dispatch_entry_mut!(self.get_mut(idx).unwrap(), |node, idx| {
                    pos = idx.unwrap();
                    let prev = node.get(pos).unwrap().get_internal().unwrap();
                    node.replace_at(
                        Entry::Internal(Internal {
                            block: updated_start,
                            next_node: prev.next_node,
                        }),
                        pos,
                    )
                    .unwrap_or_else(|_| unreachable!());
                });
            }

            Ok(None)
        })
    }

    fn grow_extent(
        &mut self,
        ino: InodeNumber,
        tx: &Transaction,
        fs: &FileSystem<C, BLK_SIZE>,
    ) -> Result<(), FsError> {
        let root = &mut self.root.0;

        let hope = root
            .get(0)
            .and_then(|en| en.next_node())
            .unwrap_or(LogicalBlockNumber(
                (ino.0 as u64 - 1) / fs.sb.inodes_per_group as u64,
            ));
        let lba = fs.blocks.allocate(self.ino, 1, hope, fs, tx)?.0;
        let b = fs.blocks.get_mut_noload(lba, &tx.collector)?;
        {
            // Initialize the node.
            let mut guard = b.write();
            let mut rw = ByteRw::new(guard.as_mut());
            rw.write_u16(0, 0xF30A);
            rw.write_u16(2, 0);
            rw.write_u16(4, ((BLK_SIZE - 16) / 12) as u16);
            rw.write_u16(6, root.get_depth());
        }

        let mut node = Node::from_bytes(b).unwrap();
        for i in 0..root.get_entries_cnt() as usize {
            node.insert_at(root.get(i).unwrap(), i)
                .unwrap_or_else(|_| unreachable!());
        }

        root.set_entries_cnt(0);
        root.set_depth(root.get_depth() + 1);
        root.insert_at(
            Entry::Internal(Internal {
                block: node.get(0).unwrap().first_block(),
                next_node: lba,
            }),
            0,
        )
        .unwrap_or_else(|_| unreachable!());
        Ok(())
    }

    fn insert_internal(&mut self, ext: Internal, at: usize) -> bool {
        // Find location for insertion.
        dispatch_entry_mut!(self.get_mut(at).unwrap(), |node, idx| {
            let loc = if node.is_full() {
                return false;
            } else if idx.is_none() {
                0
            } else if ext.block > node.get(idx.unwrap()).unwrap().first_block() {
                idx.unwrap() + 1
            } else {
                idx.unwrap()
            };
            node.insert_at(Entry::Internal(ext), loc)
                .unwrap_or_else(|_| unreachable!());
            true
        })
    }

    fn split_at(
        &mut self,
        new: &Leaf,
        at: usize,
        tx: &'b Transaction,
        fs: &'a FileSystem<C, BLK_SIZE>,
    ) -> Result<(bool, Vec<BlockRef<'a, 'b, C, BLK_SIZE, true>>), FsError> {
        let insert_idx = dispatch_entry!(self.last(), |en, idx| {
            if *idx == Some(en.get_max_entries_cnt() as usize - 1) {
                new.block
            } else {
                en.get(idx.unwrap() + 1)
                    .ok_or(FsError::InvalidFs("Corrupted Extent Tree"))?
                    .first_block()
            }
        });

        let mut blocks = Vec::with_capacity(self.len() - at);
        for _ in at..self.len() {
            let (lba, _) = fs
                .blocks
                .allocate(self.ino, 1, LogicalBlockNumber(0), fs, tx)?;
            let e = fs.blocks.get_mut_noload(lba, &tx.collector)?;
            blocks.push(e);
        }

        let depth = self.len() - 1;
        let blocks = blocks
            .into_iter()
            .enumerate()
            .map(|(d, hdr_b)| {
                // Leaf.
                dispatch_entry_mut!(self.get_mut(d + at).unwrap(), |ext, idx| {
                    // Leaf node.
                    if d + at == depth {
                        {
                            // Init node.
                            let mut guard = hdr_b.write();
                            let mut rw = ByteRw::new(guard.as_mut());
                            rw.write_u16(0, 0xF30A);
                            rw.write_u16(2, 0);
                            rw.write_u16(4, ((BLK_SIZE - 16) / 12) as u16);
                            rw.write_u16(6, ext.get_depth());
                        }

                        let mut node = Node::from_bytes(hdr_b).unwrap();
                        let base = idx.unwrap();
                        let move_amount = ext.get_entries_cnt() as usize - base - 1;
                        for i in 0..move_amount as usize {
                            node.insert_at(ext.get(base + i + 1).unwrap(), i)
                                .unwrap_or_else(|_| unreachable!());
                        }
                        ext.set_entries_cnt(base as u16);
                        node.into_inner()
                    } else {
                        todo!()
                    }
                })
            })
            .collect::<Vec<_>>();

        self.insert_internal(
            Internal {
                next_node: blocks.first().unwrap().lba(),
                block: insert_idx,
            },
            at - 1,
        )
        .then(|| (new.block >= insert_idx, blocks))
        .ok_or(FsError::InvalidFs("Extent Tree is corrupted."))
    }

    fn do_insert(
        &mut self,
        mut new: Leaf,
        ino: InodeNumber,
        tx: &'b Transaction,
        fs: &'a FileSystem<C, BLK_SIZE>,
    ) -> Result<Option<Leaf>, FsError> {
        loop {
            if let Some(l) = self.insert_leaf(new)? {
                new = l;
            } else {
                break Ok(None);
            }

            let level = if let Some(level) = (0..self.len()).rev().find(|idx| {
                self.get(*idx)
                    .map(|n| dispatch_entry!(n, |en, _idx| !en.is_full()))
                    .unwrap()
            }) {
                level
            } else {
                self.grow_extent(ino, tx, fs)?;
                break Ok(Some(new));
            };

            let (need_fix, blks) = self
                .split_at(&new, self.len() - level - 1, tx, fs)
                .map_err(|_| FsError::IoError)?;

            if need_fix {
                dispatch_entry_mut!(self.get_mut(level).unwrap(), |_node, idx| {
                    *idx = Some(idx.map(|n| n + 1).unwrap_or(0));
                });

                for blk in blks.into_iter() {
                    let (node, idx) = self.leafs.get_mut(level).unwrap();
                    *node = Node::from_bytes(blk).unwrap();
                    *idx = if node.get_entries_cnt() == 0 {
                        None
                    } else {
                        Some(node.get_entries_cnt() as usize - 1)
                    };
                }
            }
        }
    }

    pub(super) fn insert(
        &mut self,
        mut leaf: Leaf,
        ino: InodeNumber,
        tx: &'b Transaction,
        fs: &'a FileSystem<C, BLK_SIZE>,
    ) -> Result<LogicalBlockNumber, FsError> {
        let (lba, fba) = (leaf.start, leaf.block);

        while let Some(l) = self.do_insert(leaf, ino, tx, fs)? {
            leaf = l;
            self.load(fba, tx, fs)?;
        }
        Ok(lba)
    }

    pub fn load_path_to_last(
        &mut self,
        tx: &'b Transaction,
        fs: &'a FileSystem<C, BLK_SIZE>,
    ) -> Result<(), FsError> {
        let Self { root, leafs, .. } = self;
        leafs.clear();

        let root_index = (root.0.entries_cnt as usize).checked_sub(1);
        root.1 = root_index;

        let mut last_node = root_index.and_then(|idx| root.0.get(idx));

        for _ in (0..root.0.get_depth()).rev() {
            if let Some(ext) = last_node.take() {
                let node = fs
                    .blocks
                    .get_mut(ext.next_node().unwrap(), &tx.collector)
                    .and_then(Node::from_bytes)?;
                let index = Some(node.get_entries_cnt() as usize - 1);
                last_node = index.and_then(|idx| node.get(idx));
                leafs.push((node, index))
            } else {
                break;
            }
        }
        Ok(())
    }

    #[inline]
    pub(super) fn truncate_last(
        &mut self,
        new_len: u16,
        tx: &'b Transaction,
        fs: &'a FileSystem<C, BLK_SIZE>,
    ) -> Result<(), FsError> {
        let leaf = self.get_leaf().unwrap();
        #[cfg(feature = "extent_cache")]
        {
            self.cache = None;
        }
        if leaf.is_init {
            fs.blocks.deallocate(
                self.ino,
                LogicalBlockNumber(leaf.start.0 + new_len as u64),
                (leaf.len - new_len) as usize,
                fs,
                tx,
            );
        }
        dispatch_entry_mut!(self.last_mut(), |node, idx| {
            if new_len != 0 {
                let idx = idx.unwrap();
                node.replace_at(
                    Entry::Leaf(Leaf {
                        len: new_len,
                        ..leaf
                    }),
                    idx,
                )
                .unwrap();
            } else {
                node.set_entries_cnt(node.get_entries_cnt().checked_sub(1).unwrap_or_default());
                self.move_prev_with_cleanup(tx, fs)?;
            }
        });
        Ok(())
    }

    fn move_prev_with_cleanup(
        &mut self,
        tx: &'b Transaction,
        fs: &'a FileSystem<C, BLK_SIZE>,
    ) -> Result<(), FsError> {
        let orig_len = self.len() as u16;
        let mut len = self.len() as u16;
        loop {
            let idx = dispatch_entry!(self.last(), |_n, idx| *idx);
            match idx {
                Some(0) if len != 1 => {
                    let (p_node, _) = self.leafs.pop().unwrap();
                    len -= 1;
                    dispatch_entry!(self.last(), |node, idx| {
                        // cleanup
                        if p_node.get_entries_cnt() == 0 {
                            assert_eq!(
                                idx.map(|n| n + 1).unwrap(),
                                node.get_entries_cnt() as usize
                            );

                            let lba = p_node.into_inner().lba();
                            assert_eq!(node.get(idx.unwrap()).unwrap().next_node().unwrap(), lba);
                            fs.blocks.deallocate(self.ino, lba, 1, fs, tx);
                        }
                    });
                }
                Some(0) if len == 1 => {
                    dispatch_entry_mut!(self.last_mut(), |_n, idx| *idx = None);
                }
                Some(n) => {
                    dispatch_entry_mut!(self.last_mut(), |_n, idx| *idx = Some(n - 1));
                    break;
                }
                None => return Ok(()),
            }
        }
        // Refill the path
        let mut last_node =
            dispatch_entry!(self.last(), |node, idx| idx.and_then(|idx| node.get(idx)));
        for _ in len..orig_len {
            if let Some(ext) = last_node.take() {
                let node = fs
                    .blocks
                    .get_mut(ext.next_node().unwrap(), &tx.collector)
                    .and_then(Node::from_bytes)?;
                let index = Some(node.get_entries_cnt() as usize - 1);
                last_node = index.and_then(|idx| node.get(idx));
                self.leafs.push((node, index))
            } else {
                break;
            }
        }
        Ok(())
    }
}
