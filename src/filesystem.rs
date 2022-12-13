use crate::block;
use crate::block_group::BlockGroup;
use crate::directory::Directory;
use crate::inode;
use crate::superblock::SuperBlock;
use crate::transaction::{Events, Transaction};
use crate::types::BlockGroupId;
use crate::{Config, FileType, FsError, FsObject, InodeNumber};
use alloc::collections::LinkedList;
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::cell::RefCell;

pub struct FileSystem<C: Config, const BLK_SIZE: usize> {
    pub(crate) sb: SuperBlock<C, BLK_SIZE>,

    block_groups: Vec<BlockGroup<BLK_SIZE>>,

    pub(crate) blocks: block::Manager<C, BLK_SIZE>,
    pub(crate) inodes: inode::Manager<C, BLK_SIZE>,
}

impl<C: Config, const BLK_SIZE: usize> FileSystem<C, BLK_SIZE> {
    #[inline]
    pub fn conf(&self) -> &C {
        &self.blocks.conf
    }
    #[inline]
    pub fn conf_mut(&mut self) -> &mut C {
        &mut self.blocks.conf
    }

    fn make_fs_ctxt(mut self: Arc<Self>) -> Result<Arc<Self>, FsError> {
        let mut bgs = Vec::with_capacity(self.sb.bg_count as usize);
        for bgid in (0..self.sb.bg_count).map(BlockGroupId) {
            let tx = self.open_transaction();
            let (lba, index) = bgid.into_lba_index(&self.sb);
            let bg_arr = self.blocks.get_mut(lba, &tx.collector)?;
            bgs.push(BlockGroup::from_disk(
                bg_arr,
                index..index + self.sb.block_desc_size,
                bgid,
                &self,
                &tx,
            )?);
            tx.done(&self)?;
            // We don't need to hold blockgroup blocks, as they are loaded on memory.
            self.blocks.blocks.flush();
        }
        {
            let Self {
                block_groups,
                blocks,
                inodes,
                ..
            } = Arc::get_mut(&mut self).unwrap();
            *block_groups = bgs;
            inodes.load_bitmap(block_groups, blocks)?;
        }

        self.blocks.build_buddy(&self)?;
        Ok(self)
    }

    #[inline]
    pub fn get_inode_as_fs_object(
        self: &Arc<Self>,
        ino: InodeNumber,
        hint: Option<FileType>,
    ) -> Result<FsObject<C, BLK_SIZE>, FsError> {
        self.inodes.get(self, ino, hint).map(|n| n.into_fs_object())
    }

    #[inline]
    pub fn allocate_inode_as_fs_object<'a>(
        self: &'a Arc<Self>,
        ftype: FileType,
        tx: &Transaction,
    ) -> Result<(InodeNumber, FsObject<C, BLK_SIZE>), crate::FsError> {
        self.inodes
            .allocate(self, ftype, tx)
            .map(|(ino, n)| (ino, n.into_fs_object()))
    }

    #[inline]
    pub fn root(self: &Arc<Self>) -> Result<Directory<C, BLK_SIZE>, FsError> {
        const EXT4_INODE_ROOT_INDEX: InodeNumber = InodeNumber(2);

        self.inodes
            .get(self, EXT4_INODE_ROOT_INDEX, None)?
            .into_fs_object()
            .get_directory()
            .ok_or(FsError::NotDirectory)
    }

    #[inline]
    pub fn get_block_group(&self, bgid: BlockGroupId) -> &BlockGroup<BLK_SIZE> {
        &self.block_groups[bgid.0 as usize]
    }

    // Fixme: Lock
    #[inline]
    pub fn open_transaction(&self) -> Transaction {
        Transaction {
            events: Events {
                inner: Some(RefCell::new(LinkedList::new())),
            },
            collector: crate::transaction::Collector::default(),
        }
    }

    /// Open a file sytem from the device `IO`.
    pub fn new(
        conf: C,
        sb: SuperBlock<C, BLK_SIZE>,
    ) -> Result<Arc<FileSystem<C, BLK_SIZE>>, FsError> {
        let blocks = block::Manager::new(&sb, conf);
        let inodes = inode::Manager::new(&sb);
        Arc::new(FileSystem {
            sb,
            blocks,
            block_groups: Vec::new(),
            inodes,
        })
        .make_fs_ctxt()
    }
}
