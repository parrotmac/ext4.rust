use core::hash::Hasher;

#[derive(Clone, Copy, num_enum::TryFromPrimitive)]
#[repr(u8)]
pub enum HashVersion {
    Legacy = 0,
    HalfMd4 = 1,
    Tea = 2,
}

impl HashVersion {
    pub fn get_hasher(self, seed: [u32; 4], is_unsigned: bool) -> Ext4Hasher {
        match self {
            Self::Legacy if is_unsigned => Ext4Hasher::new(seed, &do_legacy_unsigned_hash),
            Self::Legacy => Ext4Hasher::new(seed, &do_legacy_hash),
            Self::HalfMd4 if is_unsigned => Ext4Hasher::new(seed, &half_md4::do_unsigned_hash),
            Self::HalfMd4 => Ext4Hasher::new(seed, &half_md4::do_signed_hash),
            Self::Tea if is_unsigned => Ext4Hasher::new(seed, &do_tea_unsigned_hash),
            Self::Tea => Ext4Hasher::new(seed, &do_tea_hash),
        }
    }
}

fn do_legacy_unsigned_hash(_hasher: &mut Ext4Hasher, _bytes: &[u8]) {
    todo!()
}

fn do_legacy_hash(_hasher: &mut Ext4Hasher, _bytes: &[u8]) {
    todo!()
}

#[inline]
fn prepare_hash_buf<const SIZE: usize>(bytes: &[u8], is_unsigned: bool) -> [u32; SIZE] {
    let mut dst = [0; SIZE];
    let slen = bytes.len();
    let len = core::cmp::min(slen, core::mem::size_of::<i32>() * SIZE);
    let padding = {
        let slen = slen as u32;
        slen | (slen << 8) | (slen << 16) | (slen << 24)
    };

    let mut dpos = 0;
    let mut buf_val = padding;

    for (i, byte) in bytes.iter().enumerate().take(len) {
        let byte = if is_unsigned {
            *byte as u8 as u32
        } else {
            *byte as i8 as i32 as u32
        };
        if i.trailing_zeros() >= 2 {
            buf_val = padding;
        }
        buf_val <<= 8;
        buf_val += byte;

        if i.trailing_ones() >= 2 {
            dst[dpos] = buf_val;
            dpos += 1;
            buf_val = padding;
        }
    }

    if let Some(n) = dst.get_mut(dpos) {
        *n = buf_val;
        if dpos + 1 < SIZE {
            dst[dpos + 1..].iter_mut().for_each(|i| *i = padding);
        }
    }
    dst
}

mod half_md4 {
    use super::Ext4Hasher;

    #[allow(clippy::many_single_char_names)]
    fn half_md4(hash: &mut [u32; 4], data: [u32; 8]) {
        #[inline]
        fn f(x: u32, y: u32, z: u32) -> u32 {
            (x & y) | (!x & z)
        }

        #[inline]
        fn g(x: u32, y: u32, z: u32) -> u32 {
            (x & y) | (x & z) | (y & z)
        }

        #[inline]
        fn h(x: u32, y: u32, z: u32) -> u32 {
            x ^ y ^ z
        }

        #[inline]
        fn ff(a: u32, b: u32, c: u32, d: u32, k: u32, s: u32) -> u32 {
            a.wrapping_add(f(b, c, d)).wrapping_add(k).rotate_left(s)
        }

        #[inline]
        fn gg(a: u32, b: u32, c: u32, d: u32, k: u32, s: u32) -> u32 {
            a.wrapping_add(g(b, c, d))
                .wrapping_add(k)
                .wrapping_add(0x5A82_7999)
                .rotate_left(s)
        }

        #[inline]
        fn hh(a: u32, b: u32, c: u32, d: u32, k: u32, s: u32) -> u32 {
            a.wrapping_add(h(b, c, d))
                .wrapping_add(k)
                .wrapping_add(0x6ED9_EBA1)
                .rotate_left(s)
        }

        let (mut a, mut b, mut c, mut d) = (hash[0], hash[1], hash[2], hash[3]);

        // Round 1.
        a = ff(a, b, c, d, data[0], 3);
        d = ff(d, a, b, c, data[1], 7);
        c = ff(c, d, a, b, data[2], 11);
        b = ff(b, c, d, a, data[3], 19);
        a = ff(a, b, c, d, data[4], 3);
        d = ff(d, a, b, c, data[5], 7);
        c = ff(c, d, a, b, data[6], 11);
        b = ff(b, c, d, a, data[7], 19);
        // Round 2.
        a = gg(a, b, c, d, data[1], 3);
        d = gg(d, a, b, c, data[3], 5);
        c = gg(c, d, a, b, data[5], 9);
        b = gg(b, c, d, a, data[7], 13);
        a = gg(a, b, c, d, data[0], 3);
        d = gg(d, a, b, c, data[2], 5);
        c = gg(c, d, a, b, data[4], 9);
        b = gg(b, c, d, a, data[6], 13);
        // Round 3.
        a = hh(a, b, c, d, data[3], 3);
        d = hh(d, a, b, c, data[7], 9);
        c = hh(c, d, a, b, data[2], 11);
        b = hh(b, c, d, a, data[6], 15);
        a = hh(a, b, c, d, data[1], 3);
        d = hh(d, a, b, c, data[5], 9);
        c = hh(c, d, a, b, data[0], 11);
        b = hh(b, c, d, a, data[4], 15);

        hash[0] = hash[0].overflowing_add(a).0;
        hash[1] = hash[1].overflowing_add(b).0;
        hash[2] = hash[2].overflowing_add(c).0;
        hash[3] = hash[3].overflowing_add(d).0;
    }

    fn do_hash(hasher: &mut Ext4Hasher, mut bytes: &[u8], is_unsigned: bool) {
        loop {
            let data = super::prepare_hash_buf::<8>(bytes, is_unsigned);
            half_md4(&mut hasher.hash, data);
            if bytes.len() <= 32 {
                break;
            } else {
                bytes = &bytes[32..];
            }
        }
        hasher.major = hasher.hash[1];
        hasher.minor = hasher.hash[2];
    }

    pub fn do_unsigned_hash(hasher: &mut Ext4Hasher, bytes: &[u8]) {
        do_hash(hasher, bytes, true)
    }

    pub fn do_signed_hash(hasher: &mut Ext4Hasher, bytes: &[u8]) {
        do_hash(hasher, bytes, false)
    }
}

fn do_tea_unsigned_hash(_hasher: &mut Ext4Hasher, _bytes: &[u8]) {
    todo!()
}

fn do_tea_hash(_hasher: &mut Ext4Hasher, _bytes: &[u8]) {
    todo!()
}

#[derive(Clone, Copy)]
pub struct Ext4Hasher {
    hash: [u32; 4],
    major: u32,
    minor: u32,
    do_hash: &'static dyn Fn(&mut Ext4Hasher, &[u8]),
}

impl Ext4Hasher {
    pub fn new(seed: [u32; 4], do_hash: &'static impl Fn(&mut Ext4Hasher, &[u8])) -> Self {
        Self {
            hash: seed,
            major: 0,
            minor: 0,
            do_hash,
        }
    }
}

impl Hasher for Ext4Hasher {
    fn write(&mut self, bytes: &[u8]) {
        (self.do_hash)(self, bytes)
    }

    fn finish(&self) -> u64 {
        const EXT2_HTREE_EOF: u32 = 0x7FFFFFFF;
        let (major, minor) = (self.major & 0xFFFFFFFE, self.minor);

        if major == EXT2_HTREE_EOF << 1 {
            ((((EXT2_HTREE_EOF - 1) << 1) as u64) << 32) | (minor as u64)
        } else {
            ((major as u64) << 32) | (minor as u64)
        }
    }
}
