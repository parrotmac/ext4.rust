mod cli;
mod shell;

use std::fs::OpenOptions;
use std::path::Path;

fn do_shell(path: &str) {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(Path::new(path))
        .expect("Failed to open device.");

    match ext4::open_fs(file).unwrap() {
        ext4::FsBlkSizeDispatch::Blk1024(fs) => shell::Shell::new(fs).run(),
        ext4::FsBlkSizeDispatch::Blk2048(fs) => shell::Shell::new(fs).run(),
        ext4::FsBlkSizeDispatch::Blk4096(fs) => shell::Shell::new(fs).run(),
    }
}

fn main() {
    use clap::Parser;
    use cli::{Command, FormatArg, MainArg, PathArg};

    match MainArg::parse().command {
        Command::Shell(PathArg { path }) => do_shell(&path),
        Command::Format(FormatArg {
            path,
            block,
            create,
        }) => linux_driver::format(&path, block, create).expect("Failed to format the device."),
        Command::Fuzz => todo!(),
    };
}
