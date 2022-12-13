use ext4::{FileSystem, FsError};
use path::{Path, PathBuf};
use std::io::{self, Write};
use std::sync::{Arc, Weak};

use fs_core::FileType;
use linux_driver::{createat, mkdirat, openat, read, write};

use colored::Colorize;

type Directory<const BLK_SIZE: usize> = ext4::Directory<std::fs::File, BLK_SIZE>;
type File<const BLK_SIZE: usize> = ext4::File<std::fs::File, BLK_SIZE>;
type FsObject<const BLK_SIZE: usize> = ext4::FsObject<std::fs::File, BLK_SIZE>;

// check if directory has only two entry_path, . and ..
fn is_directory_empty<const BLK_SIZE: usize>(dir: &Directory<BLK_SIZE>) -> bool {
    let mut pos = 0_usize;
    let mut i = 0;
    loop {
        match dir.read_dir(pos).unwrap() {
            Some((_de, pos_next)) if i < 2 => {
                pos = pos_next;
                i += 1;
            }
            Some(_) if i == 2 => {
                break false;
            }
            _ => {
                break true;
            }
        }
    }
}

pub struct Shell<const BLK_SIZE: usize> {
    fs: Arc<FileSystem<std::fs::File, BLK_SIZE>>,
    cwd: Directory<BLK_SIZE>,
    cwd_path: PathBuf,
}

impl<const BLK_SIZE: usize> Shell<BLK_SIZE> {
    pub fn new(fs: Arc<FileSystem<std::fs::File, BLK_SIZE>>) -> Self {
        let cwd = fs.root().unwrap();
        Shell {
            fs,
            cwd,
            cwd_path: PathBuf::from("/"),
        }
    }

    pub fn run(&mut self) {
        'r: loop {
            print!("{}$ ", self.cwd_path.to_str().unwrap().blue().bold());
            io::stdout().flush().unwrap();

            let mut lines = String::new();
            io::stdin()
                .read_line(&mut lines)
                .expect("EOF reached while reading from stdin");

            for cmd in lines.split(';') {
                let mut args = cmd.split_whitespace();
                if let Some(cmd) = args.next() {
                    let args = args.collect();
                    match cmd {
                        "cd" => self.cd(args), // 1 arg only
                        "ls" => self.ls(args), // multiple args
                        "cp" => self.cp(args), // (src tgt) or (src0, src1, ... , tgt_dir)
                        "mv" => self.mv(args), // (src tgt) or (src0, src1, ... , tgt_dir)
                        "pwd" => {
                            println!("{}", self.cwd_path.to_str().unwrap());
                        }
                        "mkdir" => self.mkdir(args), // multiple args
                        "rmdir" => self.rmdir(args), // multiple args
                        "touch" => self.touch(args), // multiple args
                        "rm" => self.rm(args),       // multiple args
                        "cat" => self.cat(args),     // multiple args
                        "quite" | "q" | "exit" => break 'r,
                        _ => eprintln!("{}: command not found", cmd),
                    }
                }
            }
        }
    }

    // FIXME
    //  check if entry exists
    //  for example, /existing_path/none_existing_path/.. is recognized as valid path
    pub fn abspath(&self, path: &str) -> PathBuf {
        let mut base = PathBuf::from(&self.cwd_path);
        base.push(path);

        let mut ret = PathBuf::from("/");
        for p in base.into_iter() {
            match p {
                "." => (),
                ".." => {
                    ret.pop();
                }
                e => {
                    ret.push(e);
                }
            }
        }
        ret
    }

    fn open_parent(&self, target: &str) -> Result<(Option<String>, Directory<BLK_SIZE>), FsError> {
        let target = Path::new(target);
        let mut path_iter = target.iter().peekable();

        let mut dir = if path_iter.peek() == Some(&"/") {
            path_iter.next();
            self.fs.root().unwrap()
        } else {
            self.cwd.clone()
        };
        while let Some(entry) = path_iter.next() {
            if path_iter.peek().is_none() {
                return Ok((Some(entry.to_string()), dir));
            }

            match dir.open_entry(entry)? {
                FsObject::Directory(ndir) => dir = ndir,
                FsObject::File(_) => return Err(FsError::NotDirectory),
            }
        }
        Ok((None, dir))
    }

    pub fn cd(&mut self, mut args: Vec<&str>) {
        let target = if args.len() > 1 {
            return eprintln!("cd: too many arguments");
        } else {
            args.pop().unwrap_or("/")
        };

        if let Err(e) = openat(&self.cwd, target)
            .and_then(|o| o.get_directory().ok_or(FsError::NotDirectory))
            .map(|cwd| {
                self.cwd_path = self.abspath(target);
                self.cwd = cwd;
            })
        {
            eprintln!("cd: cannot cd to {}: {}", target, display_error(e))
        }
    }

    fn listdir(&self, path: &str) -> Result<Vec<fs_core::DirEntry>, FsError> {
        let dir = openat(&self.cwd, path)?
            .get_directory()
            .ok_or(FsError::NotDirectory)?;
        let mut pos = 0;
        let mut entries = vec![];
        while let Some((de, pos_next)) = dir.read_dir(pos).unwrap() {
            pos = pos_next;
            entries.push(de);
        }
        Ok(entries)
    }

    pub fn ls(&mut self, args: Vec<&str>) {
        fn display_entries(entries: Vec<fs_core::DirEntry>) {
            for entry in entries.into_iter() {
                let path = entry.path.to_str().unwrap();
                if !path.starts_with('.') {
                    if entry.ty == FileType::Directory {
                        print!("{} ", path.blue().bold());
                    } else {
                        print!("{} ", path);
                    };
                }
            }
            println!();
        }

        if args.is_empty() {
            match self.listdir(".") {
                Ok(d) => display_entries(d),
                Err(e) => eprintln!("ls: cannot access '.': {}", display_error(e)),
            }
        } else {
            for path in args.iter() {
                match self.listdir(path) {
                    Ok(d) => {
                        println!("{}:", path);
                        display_entries(d);
                    }
                    Err(e) => eprintln!("ls: cannot access '{}': {}", path, display_error(e)),
                }
            }
        }
    }

    fn do_copy(&self, mut src: File<BLK_SIZE>, parent: &Directory<BLK_SIZE>, fname: &str) {
        let fs = Weak::upgrade(&self.cwd.get_inode().fs)
            .ok_or(FsError::Shutdown)
            .unwrap();
        let tx = fs.open_transaction();
        match parent.open_entry(fname) {
            Ok(FsObject::File(_)) => {
                parent.remove_entry(fname, &tx).expect("File system error");
            }
            Ok(FsObject::Directory(_)) => {
                return eprintln!(
                    "cp: cannot overwrite directory '{}' with non-directory",
                    fname
                );
            }
            _ => (),
        }

        match parent.create_entry(fname, FileType::RegularFile, &tx) {
            Ok(FsObject::File(mut dst)) => {
                tx.done(&fs).unwrap();
                read(&mut src)
                    .and_then(|buf| write(&mut dst, buf))
                    .expect("Failed to write");
            }
            Ok(_) => unreachable!(),
            Err(err) => {
                eprintln!("cp: failed to create file: {}", display_error(err));
            }
        }
    }

    pub fn cp(&mut self, mut args: Vec<&str>) {
        if let Some(dst) = args.pop() {
            if args.is_empty() {
                eprintln!("cp: missing destination file operand after '{}'", args[0]);
            } else if args.len() == 1 {
                let (parent, fname) = match self.open_parent(dst) {
                    Ok((Some(fname), parent)) => {
                        if let Ok(FsObject::Directory(parent)) = parent.open_entry(&fname) {
                            // cp a ../ => cp a ../a
                            (parent, Path::new(args[0]).file_name().unwrap().to_string())
                        } else {
                            (parent, fname)
                        }
                    }
                    Ok((_, parent)) => {
                        (parent, Path::new(args[0]).file_name().unwrap().to_string())
                    }
                    Err(err) => {
                        return eprintln!("cp: cannot create '{}' : {}", dst, display_error(err))
                    }
                };
                match openat(&self.cwd, args[0]).and_then(|o| o.get_file().ok_or(FsError::NotFile))
                {
                    Ok(src_file) => {
                        self.do_copy(src_file, &parent, &fname);
                    }
                    Err(err) => {
                        eprintln!("cp: cannot access '{}': {}", args[0], display_error(err))
                    }
                }
            } else {
                let parent = match self.open_parent(dst) {
                    Ok((Some(fname), parent)) => {
                        if let Ok(FsObject::Directory(parent)) = parent.open_entry(&fname) {
                            parent
                        } else {
                            return eprintln!("cp: target '{}' is not a directory", dst);
                        }
                    }
                    Ok((_, parent)) => parent,
                    Err(err) => {
                        return eprintln!("cp: cannot create '{}' : {}", dst, display_error(err))
                    }
                };
                for target in args {
                    match openat(&self.cwd, target)
                        .and_then(|o| o.get_file().ok_or(FsError::NotFile))
                    {
                        Ok(src_file) => {
                            self.do_copy(src_file, &parent, Path::new(target).file_name().unwrap());
                        }
                        Err(err) => {
                            eprintln!("cp: cannot access '{}': {}", target, display_error(err))
                        }
                    }
                }
            }
        } else {
            eprintln!("cp: missing file operand");
        }
    }

    pub fn mv(&mut self, mut args: Vec<&str>) {
        if let Some(dst) = args.pop() {
            if args.is_empty() {
                eprintln!("mv: missing destination file operand after '{}'", args[0]);
            } else {
                let fs = Weak::upgrade(&self.cwd.get_inode().fs)
                    .ok_or(FsError::Shutdown)
                    .unwrap();
                let tx = fs.open_transaction();
                for arg in args {
                    match self.open_parent(arg) {
                        Ok((Some(fname), parent)) => {
                            let (new_entry, new_parent) = match self.open_parent(dst) {
                                Ok((Some(new_entry), new_parent)) => {
                                    match new_parent.open_entry(&new_entry) {
                                        Ok(FsObject::Directory(d)) => (fname.clone(), d),
                                        Ok(FsObject::File(_)) => {
                                            match parent.remove_entry(dst, &tx) {
                                                Err(FsError::NoEntry) => (),
                                                Err(err) => {
                                                    eprintln!(
                                                        "mv: cannot move '{}': {}",
                                                        arg,
                                                        display_error(err)
                                                    );
                                                    continue;
                                                }
                                                _ => (),
                                            }
                                            (new_entry, new_parent)
                                        }
                                        Err(_) => (new_entry, new_parent),
                                    }
                                }
                                Ok((None, new_parent)) => (fname.clone(), new_parent),
                                Err(err) => {
                                    eprintln!("mv: cannot move '{}': {}", arg, display_error(err));
                                    continue;
                                }
                            };

                            // FIXME: mkdir b; touch a; mv a b => error
                            if let Err(err) = parent.rename(&fname, &new_parent, &new_entry, &tx) {
                                eprintln!("mv: cannot move '{}': {}", arg, display_error(err))
                            }
                        }
                        Ok(_) => eprintln!("mv: cannot move '/'"),
                        Err(err) => eprintln!("mv: cannot move '{}': {}", arg, display_error(err)),
                    }
                }
                tx.done(&fs).unwrap()
            }
        } else {
            eprintln!("mv: missing file operand")
        }
    }

    pub fn mkdir(&mut self, args: Vec<&str>) {
        for target in args.into_iter() {
            if let Err(e) = mkdirat(&self.cwd, target) {
                eprintln!(
                    "mkdir: cannot create directory '{}': {}",
                    target,
                    display_error(e)
                );
            }
        }
    }

    pub fn rmdir(&mut self, args: Vec<&str>) {
        if args.is_empty() {
            return eprintln!("rm: missing file operand");
        }

        let fs = Weak::upgrade(&self.cwd.get_inode().fs)
            .ok_or(FsError::Shutdown)
            .unwrap();
        let tx = fs.open_transaction();
        for target in args.into_iter() {
            if target == "." || target == ".." {
                eprintln!("rmdir: failed to remove '{}': Invalid argument", target);
                continue;
            }

            match self.open_parent(target) {
                Ok((Some(fname), parent)) => match parent.open_entry(&fname) {
                    Ok(FsObject::File(_)) => {
                        eprintln!(
                            "rmdir: {}: {}",
                            target,
                            display_error(FsError::NotDirectory)
                        )
                    }
                    Ok(FsObject::Directory(dir)) => {
                        if !is_directory_empty(&dir) {
                            eprintln!(
                                "rmdir: failed to remove '{}': Directory is not empty",
                                target
                            );
                        } else {
                            drop(dir);
                            parent.remove_entry(target, &tx).expect("File system error");
                        }
                    }
                    Err(e) => eprintln!("rmdir: {}: {}", target, display_error(e)),
                },
                Ok(_) => eprintln!("rmdir: cannot remove '/'"),
                Err(e) => eprintln!("rmdir: {}: {}", target, display_error(e)),
            }
        }
        tx.done(&fs).unwrap()
    }

    pub fn touch(&mut self, args: Vec<&str>) {
        if args.is_empty() {
            return eprintln!("touch: missing file operand");
        }
        for target in args.into_iter() {
            if let Err(e) = createat(&self.cwd, target) {
                eprintln!("touch: {}: {}", target, display_error(e))
            }
        }
    }

    // FIXME
    //  error removing file
    pub fn rm(&mut self, args: Vec<&str>) {
        if args.is_empty() {
            return eprintln!("rm: missing file operand");
        }
        let fs = Weak::upgrade(&self.cwd.get_inode().fs)
            .ok_or(FsError::Shutdown)
            .unwrap();
        let tx = fs.open_transaction();

        for target in args.into_iter() {
            if target == "." || target == ".." {
                eprintln!("rmdir: failed to remove '{}': Invalid argument", target)
            } else {
                match self.open_parent(target) {
                    Ok((Some(fname), dir)) => match dir.open_entry(&fname) {
                        Ok(FsObject::File(f)) => {
                            drop(f);
                            dir.remove_entry(target, &tx).expect("File system error");
                        }
                        Ok(FsObject::Directory(_)) => {
                            eprintln!("rm: {}: {}", target, display_error(FsError::NotFile))
                        }
                        Err(e) => eprintln!("rm: {}: {}", target, display_error(e)),
                    },
                    Ok(_) => eprintln!("rm: cannot remove '{}'", target),
                    Err(e) => eprintln!("rm: {}: {}", target, display_error(e)),
                }
            }
        }
        tx.done(&fs).unwrap()
    }

    pub fn cat(&mut self, args: Vec<&str>) {
        if args.is_empty() {
            return eprintln!("cat: missing file operand");
        }
        for path in &args {
            if let Err(e) = openat(&self.cwd, path)
                .and_then(|o| o.get_file().ok_or(FsError::NotFile))
                .and_then(|mut f| read(&mut f))
                .map(|buf| {
                    let _ = std::io::stdout().write_all(&buf);
                })
            {
                eprintln!("cat: {}: {}", path, display_error(e));
            }
        }
    }
}

fn display_error(err: FsError) -> &'static str {
    match err {
        FsError::Busy => "File system is busy.",
        FsError::FsFull => "File system is full",
        FsError::NotFile => "Not a file",
        FsError::NotDirectory => "Not a directory",
        FsError::NoEntry => "No such file or directory",
        FsError::Exist => "File exists",
        _ => todo!("{:?}", err),
    }
}
