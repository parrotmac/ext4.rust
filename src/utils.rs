use core::convert::TryInto;

pub(crate) struct ByteRw<T>
where
    T: core::convert::AsRef<[u8]>,
{
    pub b: T,
}

impl<T> ByteRw<T>
where
    T: core::convert::AsRef<[u8]>,
{
    #[inline]
    pub fn new(b: T) -> Self {
        Self { b }
    }

    #[inline]
    pub fn read_u8(&self, p: usize) -> u8 {
        self.b.as_ref()[p]
    }

    #[inline]
    pub fn read_u16(&self, p: usize) -> u16 {
        u16::from_le_bytes(self.b.as_ref()[p..p + 2].try_into().unwrap())
    }

    #[inline]
    pub fn read_u32(&self, p: usize) -> u32 {
        u32::from_le_bytes(self.b.as_ref()[p..p + 4].try_into().unwrap())
    }

    #[inline]
    pub fn inner(&self) -> &T {
        &self.b
    }

    #[inline]
    pub fn into_inner(self) -> T {
        self.b
    }
}

impl<T> ByteRw<T>
where
    T: core::convert::AsRef<[u8]>,
    T: core::convert::AsMut<[u8]>,
{
    #[inline]
    pub fn write_u8(&mut self, p: usize, v: u8) {
        self.b.as_mut()[p] = v;
    }

    #[inline]
    pub fn write_u16(&mut self, p: usize, v: u16) {
        self.b.as_mut()[p..p + 2].copy_from_slice(&u16::to_le_bytes(v))
    }

    #[inline]
    pub fn write_u32(&mut self, p: usize, v: u32) {
        self.b.as_mut()[p..p + 4].copy_from_slice(&u32::to_le_bytes(v))
    }

    #[inline]
    pub fn inner_mut(&mut self) -> &mut T {
        &mut self.b
    }

    #[inline]
    pub fn set_bitmap(&mut self, pos: impl core::iter::Iterator<Item = usize>) {
        for p in pos {
            let (group, ofs) = (p >> 3, p & 7);
            let v = self.read_u8(group) | (1 << ofs);
            self.write_u8(group, v);
        }
    }
}

pub(crate) struct Accessor<'a, T, O, const ADDR: usize>
where
    T: core::convert::AsRef<[u8]>,
{
    rw: &'a mut ByteRw<T>,
    _t: core::marker::PhantomData<(T, O)>,
}

impl<'a, T, O, const ADDR: usize> Accessor<'a, T, O, ADDR>
where
    T: core::convert::AsRef<[u8]>,
{
    #[inline]
    pub fn new(rw: &'a mut ByteRw<T>) -> Self {
        Self {
            rw,
            _t: core::marker::PhantomData,
        }
    }
}

impl<'a, T, const ADDR: usize> Accessor<'a, T, u8, ADDR>
where
    T: core::convert::AsRef<[u8]>,
{
    #[inline]
    pub fn get(&self) -> u8 {
        self.rw.read_u8(ADDR)
    }
}

impl<'a, T, const ADDR: usize> Accessor<'a, T, u8, ADDR>
where
    T: core::convert::AsRef<[u8]>,
    T: core::convert::AsMut<[u8]>,
{
    #[inline]
    pub fn set(&mut self, v: u8) {
        self.rw.write_u8(ADDR, v)
    }
}

impl<'a, T, const ADDR: usize> Accessor<'a, T, u16, ADDR>
where
    T: core::convert::AsRef<[u8]>,
{
    #[inline]
    pub fn get(&self) -> u16 {
        self.rw.read_u16(ADDR)
    }
}

impl<'a, T, const ADDR: usize> Accessor<'a, T, u16, ADDR>
where
    T: core::convert::AsRef<[u8]>,
    T: core::convert::AsMut<[u8]>,
{
    #[inline]
    pub fn set(&mut self, v: u16) {
        self.rw.write_u16(ADDR, v)
    }
}

impl<'a, T, const ADDR: usize> Accessor<'a, T, u32, ADDR>
where
    T: core::convert::AsRef<[u8]>,
{
    #[inline]
    pub fn get(&self) -> u32 {
        self.rw.read_u32(ADDR)
    }
}

impl<'a, T, const ADDR: usize> Accessor<'a, T, u32, ADDR>
where
    T: core::convert::AsRef<[u8]>,
    T: core::convert::AsMut<[u8]>,
{
    #[inline]
    pub fn set(&mut self, v: u32) {
        self.rw.write_u32(ADDR, v)
    }
}

pub(crate) struct CompoundAccessor<'a, T, O, const LO: usize, const HI: usize>
where
    T: core::convert::AsRef<[u8]>,
{
    rw: &'a mut ByteRw<T>,
    use_hi: bool,
    _t: core::marker::PhantomData<(T, O)>,
}

impl<'a, T, O, const LO: usize, const HI: usize> CompoundAccessor<'a, T, O, LO, HI>
where
    T: core::convert::AsRef<[u8]>,
{
    #[inline]
    pub fn new(rw: &'a mut ByteRw<T>, use_hi: bool) -> Self {
        Self {
            rw,
            use_hi,
            _t: core::marker::PhantomData,
        }
    }
}

impl<'a, T, const LO: usize, const HI: usize> CompoundAccessor<'a, T, u32, LO, HI>
where
    T: core::convert::AsRef<[u8]>,
{
    #[inline]
    pub fn get(&self) -> u32 {
        merge_u16(
            if self.use_hi { self.rw.read_u16(HI) } else { 0 },
            self.rw.read_u16(LO),
        )
    }
}

impl<'a, T, const LO: usize, const HI: usize> CompoundAccessor<'a, T, u32, LO, HI>
where
    T: core::convert::AsRef<[u8]>,
    T: core::convert::AsMut<[u8]>,
{
    #[inline]
    pub fn set(&mut self, v: u32) {
        let (hi, lo) = split_u32(v);
        if self.use_hi {
            self.rw.write_u16(HI, hi);
        } else {
            assert_eq!(hi, 0);
        }
        self.rw.write_u16(LO, lo);
    }
}

impl<'a, T, const LO: usize, const HI: usize> CompoundAccessor<'a, T, u64, LO, HI>
where
    T: core::convert::AsRef<[u8]>,
{
    #[inline]
    pub fn get(&self) -> u64 {
        merge_u32(
            if self.use_hi { self.rw.read_u32(HI) } else { 0 },
            self.rw.read_u32(LO),
        )
    }
}

impl<'a, T, const LO: usize, const HI: usize> CompoundAccessor<'a, T, u64, LO, HI>
where
    T: core::convert::AsRef<[u8]>,
    T: core::convert::AsMut<[u8]>,
{
    #[inline]
    pub fn set(&mut self, v: u64) {
        let (hi, lo) = split_u64(v);
        if self.use_hi {
            self.rw.write_u32(HI, hi);
        } else {
            assert_eq!(hi, 0);
        }
        self.rw.write_u32(LO, lo);
    }
}

pub(crate) type AccessorU8<'a, T, const ADDR: usize> = Accessor<'a, T, u8, ADDR>;
pub(crate) type AccessorU16<'a, T, const ADDR: usize> = Accessor<'a, T, u16, ADDR>;
pub(crate) type AccessorU32<'a, T, const ADDR: usize> = Accessor<'a, T, u32, ADDR>;
pub(crate) type CompoundAccessorU32<'a, T, const LO: usize, const HI: usize> =
    CompoundAccessor<'a, T, u32, LO, HI>;
pub(crate) type CompoundAccessorU64<'a, T, const LO: usize, const HI: usize> =
    CompoundAccessor<'a, T, u64, LO, HI>;

#[inline]
pub(crate) fn merge_u16(hi: u16, lo: u16) -> u32 {
    ((hi as u32) << 16) | (lo as u32)
}

#[inline]
pub(crate) fn merge_u32(hi: u32, lo: u32) -> u64 {
    ((hi as u64) << 32) | (lo as u64)
}

#[inline]
pub(crate) fn split_u32(s: u32) -> (u16, u16) {
    ((s >> 16) as u16, s as u16)
}

#[inline]
pub(crate) fn split_u64(s: u64) -> (u32, u32) {
    ((s >> 32) as u32, s as u32)
}

#[macro_export(local_inner_macros)]
macro_rules! __fs_field {
    ($B:ty, $(#[$attr:meta])* $N:ident : (@$O1:literal, @$O2:literal if $P:expr), u32; $($t:tt)*) => {
        __fs_field!(@MAKE, $(#[$attr])*, $B, $N, $O1, $O2, $P, u32);
        __fs_field!($B, $($t)*);
    };
    ($B:ty, $(#[$attr:meta])* $N:ident : (@$O1:literal, @$O2:literal if $P:expr), u64; $($t:tt)*) => {
        __fs_field!(@MAKE, $(#[$attr])*, $B, $N, $O1, $O2, $P, u64);
        __fs_field!($B, $($t)*);
    };
    ($B:ty, $(#[$attr:meta])* $N:ident : @$O:literal, u8; $($t:tt)*) => {
        __fs_field!(@MAKE, $(#[$attr])*, $B, $N, $O, u8);
        __fs_field!($B, $($t)*);
    };
    ($B:ty, $(#[$attr:meta])* $N:ident : @$O:literal, u16; $($t:tt)*) => {
        __fs_field!(@MAKE, $(#[$attr])*, $B, $N, $O, u16);
        __fs_field!($B, $($t)*);
    };
    ($B:ty, $(#[$attr:meta])* $N:ident : @$O:literal, u32; $($t:tt)*) => {
        __fs_field!(@MAKE, $(#[$attr])*, $B, $N, $O, u32);
        __fs_field!($B, $($t)*);
    };
    (@MAKE, $(#[$attr:meta])*, $B:ty, $N:ident, $O1:literal, $O2:literal, $P:expr, u32) => {
        $(#[$attr])*
        #[inline]
        #[allow(dead_code)]
        pub fn $N(&mut self) -> $crate::utils::CompoundAccessorU32<$B, $O1, $O2> {
            let use_hi = $P(self);
            $crate::utils::CompoundAccessorU32::new(&mut self.rw, use_hi)
        }
    };
    (@MAKE, $(#[$attr:meta])*, $B:ty, $N:ident, $O1:literal, $O2:literal, $P:expr, u64) => {
        $(#[$attr])*
        #[inline]
        #[allow(dead_code)]
        pub fn $N(&mut self) -> $crate::utils::CompoundAccessorU64<$B, $O1, $O2> {
            let use_hi = $P(self);
            $crate::utils::CompoundAccessorU64::new(&mut self.rw, use_hi)
        }
    };
    (@MAKE, $(#[$attr:meta])*, $B:ty, $N:ident, $O:literal, u8) => {
        $(#[$attr])*
        #[inline]
        #[allow(dead_code)]
        pub fn $N(&mut self) -> $crate::utils::AccessorU8<$B, $O> {
            $crate::utils::AccessorU8::new(&mut self.rw)
        }
    };
    (@MAKE, $(#[$attr:meta])*, $B:ty, $N:ident, $O:literal, u16) => {
        $(#[$attr])*
        #[inline]
        #[allow(dead_code)]
        pub fn $N(&mut self) -> $crate::utils::AccessorU16<$B, $O> {
            $crate::utils::AccessorU16::new(&mut self.rw)
        }
    };
    (@MAKE, $(#[$attr:meta])*, $B:ty, $N:ident, $O:literal, u32) => {
        $(#[$attr])*
        #[inline]
        #[allow(dead_code)]
        pub fn $N(&mut self) -> $crate::utils::AccessorU32<$B, $O> {
            $crate::utils::AccessorU32::new(&mut self.rw)
        }
    };
    ($B:ty,) => ();
}

#[macro_export]
macro_rules! fs_field {
    (ty: $B:ty; $($t:tt)*) => {
        $crate::__fs_field!($B, $($t)*);
    };
}
