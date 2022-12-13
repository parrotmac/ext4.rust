use super::node::{DirDxNode, DirDxRoot, DxEntry};
use crate::filesystem::FileSystem;
use crate::transaction::Transaction;
use crate::{Config, FsError};

pub struct HTreeCursor<
    'a,
    'l,
    'j,
    C: Config,
    const BLK_SIZE: usize,
    const R_ENTRIES: usize,
    const N_ENTRIES: usize,
    const HAS_TAIL: usize,
    const IS_MUT: bool,
> {
    pub(super) root: (
        &'a DirDxRoot<'l, 'j, C, BLK_SIZE, R_ENTRIES, N_ENTRIES, HAS_TAIL, IS_MUT>,
        usize,
    ),
    node: Option<(
        DirDxNode<'l, 'j, C, BLK_SIZE, N_ENTRIES, HAS_TAIL, IS_MUT>,
        usize,
    )>,
    hash: Option<u32>,
    check_hash: bool,
    err: Result<(), FsError>,
}

impl<
        'a,
        'l,
        'j,
        C: Config,
        const BLK_SIZE: usize,
        const R_ENTRIES: usize,
        const N_ENTRIES: usize,
        const HAS_TAIL: usize,
        const IS_MUT: bool,
    > HTreeCursor<'a, 'l, 'j, C, BLK_SIZE, R_ENTRIES, N_ENTRIES, HAS_TAIL, IS_MUT>
{
    pub fn from_index(
        root: &'a DirDxRoot<'l, 'j, C, BLK_SIZE, R_ENTRIES, N_ENTRIES, HAS_TAIL, IS_MUT>,
        index: usize,
    ) -> Option<Self> {
        if root.root_info().indirect_levels != 0 {
            todo!()
        }

        if index < root.climit().count as usize {
            Some(HTreeCursor {
                root: (root, index),
                node: None,
                hash: None,
                check_hash: false,
                err: Ok(()),
            })
        } else {
            None
        }
    }

    pub fn from_hash(
        root: &'a DirDxRoot<'l, 'j, C, BLK_SIZE, R_ENTRIES, N_ENTRIES, HAS_TAIL, IS_MUT>,
        hash: u32,
    ) -> Result<Self, FsError> {
        let ind_loop = root.root_info().indirect_levels;

        loop {
            let (mut l, mut r) = (1, root.climit().count as i32 - 1);
            while l <= r {
                let m = l + (r - l) / 2;
                // ext4_dir_idx ext4_dir_dx_get_leaf
                if root
                    .entries(m as usize)
                    .ok_or(FsError::InvalidFs("HashTree is corrupted."))?
                    .hash
                    > hash
                {
                    r = m - 1;
                } else {
                    l = m + 1;
                }
            }

            if ind_loop == 0 {
                return Ok(HTreeCursor {
                    root: (root, l as usize - 1),
                    node: None,
                    hash: Some(hash),
                    check_hash: false,
                    err: Ok(()),
                });
            }
            // 592
            todo!()
        }
    }

    pub fn try_split(&mut self) -> Result<(), FsError> {
        if let Some(_node) = &self.node {
            todo!();
        } else {
            let climit = self.root.0.climit();
            if climit.count as usize == R_ENTRIES {
                todo!()
            } else {
                Ok(())
            }
        }
    }

    pub fn has_error(&self) -> Result<(), FsError> {
        self.err
    }
}

impl<
        'a,
        'l,
        'j,
        C: Config,
        const BLK_SIZE: usize,
        const R_ENTRIES: usize,
        const N_ENTRIES: usize,
        const HAS_TAIL: usize,
    > HTreeCursor<'a, 'l, 'j, C, BLK_SIZE, R_ENTRIES, N_ENTRIES, HAS_TAIL, true>
{
    pub fn insert(
        &mut self,
        en: DxEntry,
        _fs: &FileSystem<C, BLK_SIZE>,
        _tx: &'l Transaction,
    ) -> Result<(), FsError> {
        if let Some(_node) = &self.node {
            todo!()
        } else {
            let (root, idx) = &mut self.root;
            root.insert_at(*idx, en)
                .map_err(|_| FsError::InvalidFs("Corrupted htree"))
        }
    }
}

impl<
        'a,
        'l,
        'j,
        C: Config,
        const BLK_SIZE: usize,
        const R_ENTRIES: usize,
        const N_ENTRIES: usize,
        const HAS_TAIL: usize,
        const IS_MUT: bool,
    > core::iter::Iterator
    for HTreeCursor<'a, 'l, 'j, C, BLK_SIZE, R_ENTRIES, N_ENTRIES, HAS_TAIL, IS_MUT>
{
    type Item = DxEntry;

    // ext4_dir_dx_next_block
    fn next(&mut self) -> Option<Self::Item> {
        let (hdr, pos) = &mut self.root;
        if let Some(_node_pos) = &self.node {
            todo!()
        } else if let Some(n) = hdr.entries(*pos) {
            if let Some(hash) = self.hash {
                if self.check_hash && hash + 1 != n.hash {
                    return None;
                }
                self.check_hash = true;
            }
            *pos += 1;
            Some(n)
        } else {
            None
        }
    }
}
