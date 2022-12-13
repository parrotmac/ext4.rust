# ext4.rust
Rust-only ext4 implementation without unsafe code.

## Supporting features
- `no_std`
- Direct/Indirect Block Addressing (RO)
- Extent Tree Addressing (RW)
- Htree directory
- Linear directory
- Buddy block allocator
- Multithreading

## Build & Test
```bash
$ cargo build
$ cargo test
```