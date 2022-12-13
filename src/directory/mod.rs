mod block;
mod entry;
mod htree;
mod linear;

use crate::filesystem::FileSystem;
use crate::inode::{Inode, InodeFlag};
use crate::superblock::{Ext4FeatureCompatible, Ext4FeatureReadOnly};
use crate::transaction::Transaction;
use crate::{Config, FileType, FsError, FsObject, InodeNumber};
use alloc::sync::{Arc, Weak};
use fs_core::DirEntry;

pub(crate) use block::DirectoryBlock;

pub(crate) enum Scheme<'a, C: Config, const BLK_SIZE: usize> {
    Linear(linear::LinearScheme<'a, C, BLK_SIZE>),
    HTree1024Meta(htree::HTreeScheme<'a, C, BLK_SIZE, 122, 125, 1>),
    HTree2048Meta(htree::HTreeScheme<'a, C, BLK_SIZE, 250, 253, 1>),
    HTree4096Meta(htree::HTreeScheme<'a, C, BLK_SIZE, 506, 509, 1>),
    HTree1024NoMeta(htree::HTreeScheme<'a, C, BLK_SIZE, 123, 126, 0>),
    HTree2048NoMeta(htree::HTreeScheme<'a, C, BLK_SIZE, 251, 254, 0>),
    HTree4096NoMeta(htree::HTreeScheme<'a, C, BLK_SIZE, 507, 510, 0>),
}

#[macro_export]
macro_rules! dispatch_scheme {
    ($self_:expr, |$t:ident| $code:expr) => {{
        match $self_ {
            $crate::directory::Scheme::Linear($t) => $code,
            $crate::directory::Scheme::HTree1024Meta($t) => $code,
            $crate::directory::Scheme::HTree2048Meta($t) => $code,
            $crate::directory::Scheme::HTree4096Meta($t) => $code,
            $crate::directory::Scheme::HTree1024NoMeta($t) => $code,
            $crate::directory::Scheme::HTree2048NoMeta($t) => $code,
            $crate::directory::Scheme::HTree4096NoMeta($t) => $code,
        }
    }};
}

pub struct Directory<C: Config, const BLK_SIZE: usize> {
    pub(crate) inode: Arc<Inode<C, BLK_SIZE>>,
}

impl<C: Config, const BLK_SIZE: usize> Clone for Directory<C, BLK_SIZE> {
    fn clone(&self) -> Self {
        Self {
            inode: self.inode.clone(),
        }
    }
}

impl<C: Config, const BLK_SIZE: usize> Directory<C, BLK_SIZE> {
    #[inline]
    pub fn get_inode(&self) -> &Inode<C, BLK_SIZE> {
        &self.inode
    }

    fn lookup_entry(
        &self,
        fs: &FileSystem<C, BLK_SIZE>,
        target: &str,
    ) -> Result<(InodeNumber, Option<FileType>), FsError> {
        dispatch_scheme!(self.get_scheme(fs), |s| s.find_entry(fs, target))
    }

    pub fn has_child(&self) -> Result<bool, FsError> {
        let fs = Weak::upgrade(&self.inode.fs).ok_or(FsError::Shutdown)?;
        dispatch_scheme!(self.get_scheme(&fs), |s| s.has_child(&fs))
    }

    pub fn read_dir(&self, pos: usize) -> Result<Option<(DirEntry, usize)>, FsError> {
        let fs = Weak::upgrade(&self.inode.fs).ok_or(FsError::Shutdown)?;
        dispatch_scheme!(self.get_scheme(&fs), |s| s.read_dir(&fs, pos))
    }

    pub fn open_entry(&self, entry: &str) -> Result<FsObject<C, BLK_SIZE>, FsError> {
        let fs = Weak::upgrade(&self.inode.fs).ok_or(FsError::Shutdown)?;
        let (inumber, type_) = self.lookup_entry(&fs, entry)?;
        fs.get_inode_as_fs_object(inumber, type_)
    }

    pub fn create_entry(
        &self,
        entry: &str,
        ftype: FileType,
        tx: &Transaction,
    ) -> Result<FsObject<C, BLK_SIZE>, FsError> {
        let fs = Weak::upgrade(&self.inode.fs).ok_or(FsError::Shutdown)?;
        if self.inode.rw.read().links_count == 0 {
            return Err(FsError::PendingRemoval);
        } else if self.lookup_entry(&fs, entry).is_ok() {
            return Err(FsError::Exist);
        }

        let (ino, obj) = fs.allocate_inode_as_fs_object(ftype, tx)?;
        if matches!(ftype, FileType::Directory) {
            if let FsObject::Directory(child_dir) = &obj {
                dispatch_scheme!(child_dir.get_scheme(&fs), |s| {
                    s.add_entry(&fs, ".", FileType::Directory, ino, tx)?;
                    s.add_entry(&fs, "..", FileType::Directory, self.inode.ino, tx)?;
                });
                child_dir.inode.rw.write().links_count += 1;
                tx.inode_add_link(ino);
                tx.inode_add_link(self.inode.ino);
            } else {
                unreachable!()
            }
        }

        // FIXME: apply on in-memory state first.
        dispatch_scheme!(self.get_scheme(&fs), |s| s
            .add_entry(&fs, entry, ftype, ino, tx))?;
        tx.inode_add_link(ino);
        Ok(obj)
    }

    #[inline]
    pub(crate) fn get_scheme(&self, fs: &FileSystem<C, BLK_SIZE>) -> Scheme<C, BLK_SIZE> {
        let use_htree = fs
            .sb
            .features_compatible
            .contains(Ext4FeatureCompatible::DIR_INDEX)
            && self.inode.flags.contains(InodeFlag::INDEX);
        let has_meta_csum = fs
            .sb
            .features_readonly
            .contains(Ext4FeatureReadOnly::METADATA_CSUM);
        match (BLK_SIZE, use_htree, has_meta_csum) {
            (1024, true, true) => Scheme::HTree1024Meta(htree::HTreeScheme::new(self)),
            (2048, true, true) => Scheme::HTree2048Meta(htree::HTreeScheme::new(self)),
            (4096, true, true) => Scheme::HTree4096Meta(htree::HTreeScheme::new(self)),
            (1024, true, false) => Scheme::HTree1024NoMeta(htree::HTreeScheme::new(self)),
            (2048, true, false) => Scheme::HTree2048NoMeta(htree::HTreeScheme::new(self)),
            (4096, true, false) => Scheme::HTree4096NoMeta(htree::HTreeScheme::new(self)),
            (1024, false, _) | (2048, false, _) | (4096, false, _) => {
                Scheme::Linear(linear::LinearScheme::new(self))
            }
            _ => unreachable!(),
        }
    }

    #[allow(clippy::type_complexity)]
    fn do_remove_entry(
        &self,
        entry: &str,
        fs: &Arc<FileSystem<C, BLK_SIZE>>,
        tx: &Transaction,
    ) -> Result<(InodeNumber, Arc<Inode<C, BLK_SIZE>>), FsError> {
        // FIXME: Check directory is empty
        let ino = dispatch_scheme!(self.get_scheme(fs), |s| s.remove_entry(
            fs,
            entry,
            &tx.collector
        ))?;
        let inode = fs.inodes.get(fs, ino, None)?;
        {
            let mut guard = inode.rw.write();
            match (guard.links_count, inode.ftype) {
                (1, FileType::RegularFile) => {
                    // if zero, push it to orphan file list.
                    fs.inodes.remove(ino);
                }
                (2, FileType::Directory) => {
                    // As dot point itself, we must dec link twice.
                    guard.links_count -= 1;
                    tx.inode_rm_link(ino);
                    tx.inode_rm_link(self.inode.ino);
                }
                (0, _) | (_, FileType::Directory) => {
                    // Hardlink is not permitted on directory.
                    return Err(FsError::InvalidFs("Invalid inode.links_count"));
                }
                _ => (),
            }

            // dec child->link_count
            guard.links_count -= 1;
        }
        // Add tx.
        tx.inode_rm_link(ino);
        Ok((ino, inode))
    }

    pub fn remove_entry(
        &self,
        entry: &str,
        tx: &Transaction,
    ) -> Result<(InodeNumber, Arc<Inode<C, BLK_SIZE>>), FsError> {
        // Inode must not be dropped before tx.done() is called.
        let fs = Weak::upgrade(&self.inode.fs).ok_or(FsError::Shutdown)?;
        self.do_remove_entry(entry, &fs, tx)
    }

    pub fn rename(
        &self,
        entry: &str,
        new_dir: &Self,
        new_entry: &str,
        tx: &Transaction,
    ) -> Result<(), FsError> {
        let fs = Weak::upgrade(&self.inode.fs).ok_or(FsError::Shutdown)?;
        let ino = dispatch_scheme!(self.get_scheme(&fs), |s| s.remove_entry(
            &fs,
            entry,
            &tx.collector
        ))?;
        // FIXME: One produce entry, and other remove it. Cannot progress.
        let old = match self.do_remove_entry(new_entry, &fs, tx) {
            Err(FsError::NoEntry) => None,
            e => Some(e?),
        };
        let inode = fs.inodes.get(&fs, ino, None)?;

        dispatch_scheme!(new_dir.get_scheme(&fs), |s| s.add_entry(
            &fs,
            new_entry,
            inode.ftype,
            ino,
            tx
        ))?;
        drop(old);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::tests::run_test;
    use crate::FileType;

    #[test]
    fn make_regular_file() {
        run_test(
            |fs| {
                let tx = fs.open_transaction();
                let _ = fs
                    .root()
                    .unwrap()
                    .create_entry("test", FileType::RegularFile, &tx)
                    .expect("Failed to create `/test`");
                tx.done(&fs).unwrap();
            },
            100,
        );
    }

    #[test]
    fn make_regular_file_many() {
        run_test(
            |fs| {
                let tx = fs.open_transaction();
                for i in 0..4096 {
                    let mut path = String::from("test");
                    path.extend(i.to_string().chars());

                    let _ = fs
                        .root()
                        .unwrap()
                        .create_entry(&path, FileType::RegularFile, &tx)
                        .unwrap_or_else(|err| {
                            panic!("Failed to create `{:?}`. reason: {:?}", path, err)
                        });
                }
                tx.done(&fs).unwrap();
            },
            100,
        );
    }

    #[test]
    fn make_directory() {
        run_test(
            |fs| {
                let tx = fs.open_transaction();
                let _ = fs
                    .root()
                    .unwrap()
                    .create_entry("test", FileType::Directory, &tx)
                    .expect("Failed to create `/test`");
                tx.done(&fs).unwrap();
            },
            100,
        );
    }

    #[test]
    fn make_directory_many() {
        run_test(
            |fs| {
                let tx = fs.open_transaction();
                for i in 0..1024 {
                    let mut path = String::from("test");
                    path.extend(i.to_string().chars());

                    let _ = fs
                        .root()
                        .unwrap()
                        .create_entry(&path, FileType::Directory, &tx)
                        .unwrap_or_else(|_| panic!("Failed to create `{:?}`", path));
                }
                tx.done(&fs).unwrap();
            },
            100,
        );
    }
}
