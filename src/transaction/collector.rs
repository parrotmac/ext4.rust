use crate::filesystem::FileSystem;
use crate::{BlockGroupId, Config, FsError, InodeNumber, LogicalBlockNumber};
use alloc::vec::Vec;
use core::cell::RefCell;
use core::sync::atomic::AtomicBool;
use hashbrown::HashSet;

#[derive(Debug)]
pub enum PostludeOps {
    BlockDeallocation {
        bgid: BlockGroupId,
        ofs: usize,
        count: usize,
    },
    InodeDeallocation(InodeNumber),
}

pub struct Collector {
    modifications: RefCell<HashSet<LogicalBlockNumber>>,
    pub(crate) postludes: RefCell<Vec<PostludeOps>>,
    is_sb_dirty: AtomicBool,
}

// XXX: check sb.manipulator is locked until collector is alive.
impl Default for Collector {
    #[inline]
    fn default() -> Self {
        Self {
            modifications: RefCell::new(HashSet::new()),
            postludes: RefCell::new(Vec::new()),
            is_sb_dirty: AtomicBool::new(false),
        }
    }
}

impl Collector {
    #[inline]
    pub fn track(&self, lba: LogicalBlockNumber) {
        self.modifications.borrow_mut().insert(lba);
    }

    #[inline]
    pub(crate) fn set_sb_dirty(&self) {
        self.is_sb_dirty
            .store(true, core::sync::atomic::Ordering::Relaxed);
    }

    #[inline]
    pub fn done<C: Config, const BLK_SIZE: usize>(
        self,
        fs: &FileSystem<C, BLK_SIZE>,
    ) -> Result<(), FsError> {
        let Self {
            modifications,
            postludes,
            is_sb_dirty,
        } = self;

        if is_sb_dirty.load(core::sync::atomic::Ordering::Relaxed) {
            fs.sb.manipulator.lock().writeback(&fs.blocks.conf)?;
        }
        let l = modifications.into_inner();
        let mut bio = alloc::vec::Vec::with_capacity(l.len());
        for lba in l.into_iter() {
            bio.push(fs.blocks.get_io_request(lba)?);
        }
        fs.blocks.conf.write_bios(bio)?;

        for postlude in postludes.into_inner().into_iter() {
            match postlude {
                PostludeOps::BlockDeallocation { bgid, ofs, count } => {
                    fs.blocks.apply_deallocation_on_core(bgid, ofs, count, fs)
                }
                PostludeOps::InodeDeallocation(ino) => {
                    fs.inodes.apply_deallocation_on_core(ino, fs);
                }
            }
        }
        Ok(())
    }
}
