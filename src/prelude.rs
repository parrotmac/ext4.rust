#[cfg(not(any(test, feature = "std")))]
mod inner {
    #[cfg(feature = "extern_print")]
    mod printer {
        extern "Rust" {
            pub(crate) fn _print(fmt: core::fmt::Arguments<'_>);
        }

        #[allow(dead_code, unsafe_code)]
        pub(crate) fn print(fmt: core::fmt::Arguments<'_>) {
            unsafe { _print(fmt) }
        }

        #[macro_export]
        macro_rules! println {
            () => {
                $crate::print(format_arg!("\n"))
            };
            ($($arg:tt)*) => {
                $crate::print(format_args!("{}\n", format_args!($($arg)*)))
            };
        }

        #[macro_export]
        macro_rules! print {
            ($($arg:tt)*) => {
                $crate::print(format_args!($($arg)*))
            };
        }
    }
    #[cfg(feature = "extern_print")]
    #[allow(unused_imports)]
    pub use printer::*;
    pub use synchronizations::rwlock::{RwLock, RwLockReadGuard, RwLockWriteGuard};
}
#[cfg(feature = "extern_print")]
pub use inner::*;

#[cfg(any(test, feature = "std"))]
mod inner {
    pub use std::{print, println};
    #[repr(transparent)]
    pub struct RwLock<V, D> {
        inner: std::sync::RwLock<V>,
        _p: core::marker::PhantomData<D>,
    }
    #[repr(transparent)]
    pub struct RwLockReadGuard<'a, V, D> {
        inner: std::sync::RwLockReadGuard<'a, V>,
        _p: core::marker::PhantomData<D>,
    }
    impl<'a, V, D> std::ops::Deref for RwLockReadGuard<'a, V, D> {
        type Target = V;
        fn deref(&self) -> &Self::Target {
            &*self.inner
        }
    }
    #[repr(transparent)]
    pub struct RwLockWriteGuard<'a, V, D> {
        inner: std::sync::RwLockWriteGuard<'a, V>,
        _p: std::marker::PhantomData<D>,
    }
    impl<'a, V, D> std::ops::Deref for RwLockWriteGuard<'a, V, D> {
        type Target = V;
        fn deref(&self) -> &Self::Target {
            &*self.inner
        }
    }
    impl<'a, V, D> std::ops::DerefMut for RwLockWriteGuard<'a, V, D> {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut *self.inner
        }
    }
    impl<V, D> RwLock<V, D> {
        #[inline]
        pub fn new(inner: V) -> Self {
            Self {
                inner: std::sync::RwLock::new(inner),
                _p: std::marker::PhantomData,
            }
        }
        #[inline]
        pub fn write(&self) -> RwLockWriteGuard<V, D> {
            RwLockWriteGuard {
                inner: self.inner.write().unwrap(),
                _p: std::marker::PhantomData,
            }
        }
        #[inline]
        pub fn read(&self) -> RwLockReadGuard<V, D> {
            RwLockReadGuard {
                inner: self.inner.read().unwrap(),
                _p: std::marker::PhantomData,
            }
        }
        #[inline]
        pub fn try_write(&self) -> Result<RwLockWriteGuard<V, D>, ()> {
            self.inner
                .try_write()
                .map(|inner| RwLockWriteGuard {
                    inner,
                    _p: std::marker::PhantomData,
                })
                .map_err(|_| ())
        }
        #[inline]
        pub fn try_read(&self) -> Result<RwLockReadGuard<V, D>, ()> {
            self.inner
                .try_read()
                .map(|inner| RwLockReadGuard {
                    inner,
                    _p: std::marker::PhantomData,
                })
                .map_err(|_| ())
        }
    }

    #[doc(hidden)]
    #[derive(Default, Clone)]
    pub struct SpinningDreamer {}

    use core::panic::Location;
    use core::sync::atomic::AtomicU32;

    impl synchronizations::ticket_lock::Dreamer for SpinningDreamer {
        type HookAux = ();
        #[inline]
        fn sleeping(&self, state: &AtomicU32, ticket: u32) {
            let backoff = crossbeam_utils::Backoff::new();

            while state.load(core::sync::atomic::Ordering::Relaxed) != ticket {
                backoff.spin();
            }
        }
        #[inline]
        fn waking_up(&self) {}
        #[inline]
        fn prehook(&self) -> Self::HookAux {}
        #[inline]
        fn acquire_hook(&self, _l: &Location<'static>) {}
        #[inline]
        fn release_hook(&self, _l: &Location<'static>) {}
    }

    impl synchronizations::HasConstDefault for SpinningDreamer {
        const DEFAULT: Self = Self {};
    }

    /// A Opaque dreamer for RwLock.
    ///
    /// We will not use the implementation of synchromization::RwLock on std.
    /// This is opaque object that implements synchronization::Dreamer<State=AtomicUsize>.
    #[doc(hidden)]
    #[derive(Default, Clone)]
    pub struct OpaqueDreamer {}

    impl synchronizations::rwlock::Dreamer for OpaqueDreamer {
        type HookAux = ();

        #[inline]
        fn sleeping(
            &self,
            _s: &core::sync::atomic::AtomicUsize,
            _h: synchronizations::rwlock::Hint,
        ) {
            unreachable!()
        }
        #[inline]
        fn waking_up(&self) {
            unreachable!()
        }
        #[inline]
        fn prehook(&self) -> Self::HookAux {
            unreachable!()
        }
        #[inline]
        fn acquire_hook(
            &self,
            _h: synchronizations::rwlock::Hint,

            _l: &core::panic::Location<'static>,
        ) {
            unreachable!()
        }
        #[inline]
        fn release_hook(
            &self,
            _h: synchronizations::rwlock::Hint,
            _l: &core::panic::Location<'static>,
        ) {
            unreachable!()
        }
    }

    impl synchronizations::HasConstDefault for OpaqueDreamer {
        const DEFAULT: Self = Self {};
    }
}

pub use inner::*;
pub use synchronizations::rwlock::Dreamer as RwDreamer;
pub use synchronizations::ticket_lock::{Dreamer, TicketLock, TicketLockGuard};

#[cfg(test)]
pub(crate) mod tests {
    use crate::format;
    use rand::distributions::{Alphanumeric, Distribution};
    use rand::{thread_rng, Rng};
    use std::fs::OpenOptions;
    use std::path::PathBuf;

    struct UpperHex;

    impl Distribution<u8> for UpperHex {
        fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> u8 {
            const RANGE: u32 = 16;
            const GEN_ASCII_STR_CHARSET: &[u8] = b"0123456789ABCDEF";
            loop {
                let var = rng.next_u32() >> (32 - 4);
                if var < RANGE {
                    return GEN_ASCII_STR_CHARSET[var as usize];
                }
            }
        }
    }

    pub(crate) fn test_oracle(disk: &std::path::Path) -> bool {
        use std::process::Command;

        let output = Command::new("fsck.ext4")
            .args(["-f", "-n", disk.to_str().unwrap()])
            .output()
            .unwrap();
        if output.status.success() {
            true
        } else {
            println!("{:?}", String::from_utf8_lossy(&output.stdout));
            false
        }
    }

    pub(crate) fn make_alphanumeric_string(len: usize) -> String {
        thread_rng()
            .sample_iter(&Alphanumeric)
            .take(len)
            .map(char::from)
            .collect()
    }

    pub(crate) fn make_upper_hex_string(len: usize) -> String {
        thread_rng()
            .sample_iter(&UpperHex)
            .take(len)
            .map(char::from)
            .collect()
    }

    pub(crate) fn make_disk(block_size: usize, total_size: u64) -> PathBuf {
        use std::os::unix::fs::FileExt;

        let mut disk = std::path::PathBuf::new();
        disk.push(r"/tmp");
        disk.push(format!("{}.disk", make_alphanumeric_string(8)));

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(disk.as_path())
            .expect("Failed to create file.");
        let bytes = (0..block_size as usize).map(|_| 0).collect::<Vec<u8>>();
        for b in (0..total_size).step_by(block_size as usize) {
            file.write_at(&bytes, b).expect("Failed to fill data.");
        }
        file.sync_data().expect("Failed to flush data.");

        let (uuid, volumn_name, hash_seed) = (
            make_upper_hex_string(16),
            b"AAAABBBBCCCCDDDD",
            make_upper_hex_string(16),
        );
        match block_size {
            1024 => format::format::<_, 1024>(
                file,
                uuid.as_bytes().try_into().unwrap(),
                &volumn_name,
                hash_seed.as_bytes().try_into().unwrap(),
            )
            .map(|_| ()),
            2048 => format::format::<_, 2048>(
                file,
                uuid.as_bytes().try_into().unwrap(),
                &volumn_name,
                hash_seed.as_bytes().try_into().unwrap(),
            )
            .map(|_| ()),
            4096 => format::format::<_, 4096>(
                file,
                uuid.as_bytes().try_into().unwrap(),
                &volumn_name,
                hash_seed.as_bytes().try_into().unwrap(),
            )
            .map(|_| ()),
            _ => unreachable!(),
        }
        .expect("Failed to format the device.");
        disk
    }

    pub(crate) fn run_test(
        action: impl FnOnce(alloc::sync::Arc<crate::FileSystem<std::fs::File, 4096>>),
        size_in_mi: u64,
    ) {
        const M: u64 = 1024 * 1024;

        let disk = make_disk(4096, size_in_mi * M);
        action(
            crate::open_fs(
                OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(disk.as_path())
                    .expect("failed to open disk"),
            )
            .expect("Failed to open fs"),
        );
        let r = test_oracle(&disk);
        let _ = std::fs::remove_file(&disk);
        assert!(r, "fsck failed");
    }
}
