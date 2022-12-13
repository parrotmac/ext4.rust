use crate::inode::{AddressingOutput, Inode};
use crate::transaction::Transaction;
use crate::types::Zero;
use crate::{Config, FileBlockNumber, FsError};
use alloc::sync::{Arc, Weak};
use alloc::vec::Vec;

pub struct File<C: Config, const BLK_SIZE: usize> {
    pub(crate) inode: Arc<Inode<C, BLK_SIZE>>,
}

impl<C: Config, const BLK_SIZE: usize> File<C, BLK_SIZE> {
    #[inline]
    pub fn get_inode(&self) -> &Arc<Inode<C, BLK_SIZE>> {
        &self.inode
    }

    #[inline]
    pub fn get_size(&self) -> usize {
        self.inode.get_size() as usize
    }

    #[inline]
    pub fn set_size(&self, size: usize, tx: &Transaction) {
        self.inode.set_size(size as u64, tx)
    }

    #[inline]
    pub fn for_each_lba(
        &self,
        st: FileBlockNumber,
        len: usize,
        mut f: impl FnMut(AddressingOutput) -> Result<(), FsError>,
    ) -> Result<(), FsError> {
        let fs = Weak::upgrade(&self.inode.fs).ok_or(FsError::Shutdown)?;
        dispatch_cursor!(self.inode, &fs, st, |mut cursor| {
            for address in cursor.take(len) {
                f(address)?;
            }
        });
        Ok(())
    }

    #[inline]
    pub fn for_each_lba_mut(
        &self,
        st: FileBlockNumber,
        len: usize,
        tx: &Transaction,
        mut f: impl FnMut(crate::LogicalBlockNumber) -> Result<(), FsError>,
    ) -> Result<(), FsError> {
        let fs = Weak::upgrade(&self.inode.fs).ok_or(FsError::Shutdown)?;
        dispatch_cursor_mut!(self.inode, &fs, st, tx, |mut cursor| {
            for i in 0..len {
                let lba = cursor.or_allocated(false)?;
                if i != len - 1 {
                    cursor.move_next()?;
                }
                f(lba)?;
            }
        });
        Ok(())
    }

    #[inline]
    pub fn write_slices(
        &mut self,
        ofs: usize,
        iovec: Vec<&C::Buffer<BLK_SIZE>>,
        tx: &Transaction,
    ) -> Result<(), FsError> {
        let fs = Weak::upgrade(&self.inode.fs).ok_or(FsError::Shutdown)?;
        let fba = FileBlockNumber((ofs / BLK_SIZE) as u32);
        assert_eq!(ofs & 0xfff, 0);

        self.inode.prealloc(
            fba + (iovec.len() * (4096 / BLK_SIZE))
                .try_into()
                .map_err(|_| FsError::FsFull)?,
            &fs,
            tx,
        )?;

        let len = iovec.len() * (4096 / BLK_SIZE);
        let (mut cnt, mut batch, mut start) = (0, Vec::new(), None);
        let mut slices = iovec.into_iter();
        self.for_each_lba_mut(fba, len, tx, |lba| {
            let slice = slices.next().unwrap();
            if *start.get_or_insert(lba) + cnt != lba {
                // Flush the request.
                fs.blocks
                    .conf
                    .write_vectored(start.take().unwrap().0 as usize * BLK_SIZE, &batch)
                    .map_err(|_| FsError::IoError)?;
                start = Some(lba);
                cnt = 0;
                batch.clear();
            }
            cnt += 1;
            batch.push(slice);
            Ok(())
        })?;
        if !batch.is_empty() {
            fs.blocks
                .conf
                .write_vectored(start.take().unwrap().0 as usize * BLK_SIZE, &batch)
                .map_err(|_| FsError::IoError)?;
        }
        Ok(())
    }

    #[inline]
    pub fn read_slices<B: Extend<C::Buffer<4096>>>(
        &mut self,
        iovec: &mut B,
        ofs: usize,
        len: usize,
    ) -> Result<(), FsError> {
        let fs = Weak::upgrade(&self.inode.fs).ok_or(FsError::Shutdown)?;
        let (mut cnt, mut start) = (0, None);
        assert_eq!(ofs & 0xfff, 0);

        self.for_each_lba(FileBlockNumber((ofs / BLK_SIZE) as u32), len, |addr| {
            match addr {
                AddressingOutput::Uninitialized => iovec.extend(Some(C::Buffer::<4096>::zeroed())),
                AddressingOutput::Initialized(lba) => {
                    if *start.get_or_insert(lba) + cnt != lba {
                        // Flush the request.
                        fs.blocks
                            .conf
                            .read_vectored(
                                iovec,
                                start.take().unwrap().0 as usize * BLK_SIZE,
                                cnt as usize,
                            )
                            .map_err(|_| FsError::IoError)?;
                        start = Some(lba);
                        cnt = 0;
                    }
                    cnt += 1;
                }
            }
            Ok(())
        })?;
        if cnt > 0 {
            fs.blocks
                .conf
                .read_vectored(
                    iovec,
                    start.take().unwrap().0 as usize * BLK_SIZE,
                    cnt as usize,
                )
                .map_err(|_| FsError::IoError)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::tests::run_test;
    use crate::FileType;

    #[test]
    fn write_small() {
        run_test(
            |fs| {
                let buf = Box::new([0; 4096]);
                let tx = fs.open_transaction();
                let mut file = fs
                    .root()
                    .unwrap()
                    .create_entry("test", FileType::RegularFile, &tx)
                    .expect("Failed to create `/test`")
                    .get_file()
                    .unwrap();
                file.write_slices(0, vec![&buf], &tx)
                    .expect("Failed to write.");
                file.set_size(4096, &tx);
                tx.done(&fs).unwrap();
            },
            100,
        );
    }

    #[test]
    fn write_big() {
        // const SIZE: usize = 4 * 1024 *1024 * 1024;
        const SIZE: usize = 256 * 1024 * 1024;
        run_test(
            |fs| {
                let buf = Box::new([0; 4096]);
                let tx = fs.open_transaction();
                let mut file = fs
                    .root()
                    .unwrap()
                    .create_entry("test", FileType::RegularFile, &tx)
                    .expect("Failed to create `/test`")
                    .get_file()
                    .unwrap();
                println!("write slice");
                file.write_slices(0, vec![&buf; SIZE / 4096], &tx)
                    .expect("Failed to write.");
                file.set_size(SIZE, &tx);
                println!("wb");
                tx.done(&fs).unwrap();
            },
            (SIZE as u64) / 1024 / 1024 + 100,
        );
    }
}
