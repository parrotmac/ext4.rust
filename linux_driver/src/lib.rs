use fs_core::{FileType, FsError};
use path::Path;
use std::convert::TryFrom;
use std::fs::OpenOptions;
use std::vec::Vec;

pub fn format(path: &str, block: usize, create: Option<u64>) -> Result<(), FsError> {
    use rand::Rng;
    use std::os::unix::fs::FileExt;
    use std::path::Path;

    let file = if let Some(create) = create {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(Path::new(&path))
            .expect("Failed to create file.");
        let bytes = (0..block as usize).map(|_| 0).collect::<Vec<u8>>();
        for b in (0..create).step_by(block as usize) {
            file.write_at(&bytes, b).expect("Failed to fill data.");
        }
        file.sync_data().expect("Failed to flush data.");
        file
    } else {
        OpenOptions::new()
            .read(true)
            .write(true)
            .open(Path::new(&path))
            .expect("Failed to open device.")
    };

    let mut rng = rand::thread_rng();

    let (uuid, volumn_name, hash_seed) = (
        (0..16)
            .map(|_| match rng.gen_range(0..16) {
                a if a < 10 => a + 0x30,
                a if a < 16 => a + 0x41 - 10,
                _ => unreachable!(),
            })
            .collect::<Vec<_>>()
            .try_into()
            .unwrap(),
        b"AAAABBBBCCCCDDDD",
        (0..16)
            .map(|_| rng.gen())
            .collect::<Vec<_>>()
            .try_into()
            .unwrap(),
    );
    match block {
        1024 => ext4::format::format::<_, 1024>(file, &uuid, volumn_name, &hash_seed).map(|_| ()),
        2048 => ext4::format::format::<_, 2048>(file, &uuid, volumn_name, &hash_seed).map(|_| ()),
        4096 => ext4::format::format::<_, 4096>(file, &uuid, volumn_name, &hash_seed).map(|_| ()),
        _ => unreachable!(),
    }
}

type Directory<const BLK_SIZE: usize> = ext4::Directory<std::fs::File, BLK_SIZE>;
type File<const BLK_SIZE: usize> = ext4::File<std::fs::File, BLK_SIZE>;
type FsObject<const BLK_SIZE: usize> = ext4::FsObject<std::fs::File, BLK_SIZE>;

// Dir ops
pub fn openat<P: AsRef<Path>, const BLK_SIZE: usize>(
    dir: &Directory<BLK_SIZE>,
    pathname: P,
) -> Result<FsObject<BLK_SIZE>, FsError> {
    let fs = dir.get_inode().get_fs().unwrap();
    let target = pathname.as_ref();
    let mut path_iter = target.iter().peekable();

    let mut dir = if path_iter.peek() == Some(&"/") {
        path_iter.next();
        fs.root().unwrap()
    } else {
        dir.clone()
    };
    while let Some(entry) = path_iter.next() {
        match dir.open_entry(entry)? {
            FsObject::Directory(ndir) => dir = ndir,
            f if path_iter.peek().is_none() => return Ok(f),
            _ => return Err(FsError::NotDirectory),
        }
    }
    Ok(ext4::FsObject::Directory(dir))
}

fn do_create<P: AsRef<Path>, const BLK_SIZE: usize>(
    dir: &Directory<BLK_SIZE>,
    pathname: P,
    ft: FileType,
) -> Result<(), FsError> {
    let name = pathname.as_ref().file_name().ok_or(FsError::Invalid)?;
    let fs = dir.get_inode().fs.upgrade().unwrap();
    let tx = fs.open_transaction();
    if let Some(n) = pathname.as_ref().parent() {
        openat(dir, n)?
            .get_directory()
            .ok_or(FsError::NotDirectory)?
            .create_entry(name, ft, &tx)
    } else {
        dir.create_entry(name, ft, &tx)
    }
    .and_then(|_| tx.done(&fs))
}

pub fn createat<P: AsRef<Path>, const BLK_SIZE: usize>(
    dir: &Directory<BLK_SIZE>,
    pathname: P,
) -> Result<(), FsError> {
    do_create(dir, pathname, FileType::RegularFile)
}

pub fn mkdirat<P: AsRef<Path>, const BLK_SIZE: usize>(
    dir: &Directory<BLK_SIZE>,
    pathname: P,
) -> Result<(), FsError> {
    do_create(dir, pathname, FileType::Directory)
}

pub fn unlinkat<P: AsRef<Path>, const BLK_SIZE: usize>(
    dir: &Directory<BLK_SIZE>,
    pathname: P,
) -> Result<(), FsError> {
    let name = pathname.as_ref().file_name().ok_or(FsError::Invalid)?;
    let fs = dir.get_inode().fs.upgrade().unwrap();
    let tx = fs.open_transaction();
    if let Some(n) = pathname.as_ref().parent() {
        openat(dir, n)?
            .get_directory()
            .ok_or(FsError::NotDirectory)?
            .remove_entry(name, &tx)
    } else {
        dir.remove_entry(name, &tx)
    }
    .and_then(|_| tx.done(&fs))
}

// File Ops
pub fn read<const BLK_SIZE: usize>(file: &mut File<BLK_SIZE>) -> Result<Vec<u8>, FsError> {
    let size = file.get_size();
    let mut output = Vec::new();
    file.read_slices(&mut output, 0, (size + 4095) & !4095)
        .map(|_| {
            output
                .iter()
                .map(|n| n.as_ref())
                .flatten()
                .cloned()
                .collect()
        })
}

pub fn write<const BLK_SIZE: usize>(
    file: &mut File<BLK_SIZE>,
    mut buf: Vec<u8>,
) -> Result<(), FsError> {
    let fs = file.get_inode().fs.upgrade().unwrap();
    let tx = fs.open_transaction();
    let len = buf.len();

    buf.resize((len + 4095) & !4095, 0);
    let iov = buf
        .chunks(BLK_SIZE)
        .map(|n| Box::new(<&[u8; BLK_SIZE]>::try_from(n).unwrap().clone()))
        .collect::<Vec<_>>();
    file.write_slices(0, iov.iter().collect(), &tx)
        .map(|_| file.set_size(len, &tx))
        .and_then(|_| tx.done(&fs))
}
