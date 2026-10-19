package com.aetherdb.common;

/**
 * Fast, non-cryptographic MurmurHash3 implementation (x86_32 and x64_128).
 * Optimized for Bloom filters and consistent hash ring routing.
 */
public final class MurmurHash3 {

    private static final int C1_32 = 0xcc9e2d51;
    private static final int C2_32 = 0x1b873593;

    private static final long C1_128 = 0x87c37b91114253d5L;
    private static final long C2_128 = 0x4cf5ad432745937fL;

    private MurmurHash3() {}

    /**
     * MurmurHash3 32-bit variant for byte arrays.
     */
    public static int hash32(byte[] data, int offset, int length, int seed) {
        int h1 = seed;
        int roundedEnd = offset + (length & 0xfffffffc);

        for (int i = offset; i < roundedEnd; i += 4) {
            int k1 = (data[i] & 0xff)
                    | ((data[i + 1] & 0xff) << 8)
                    | ((data[i + 2] & 0xff) << 16)
                    | (data[i + 3] << 24);

            k1 *= C1_32;
            k1 = Integer.rotateLeft(k1, 15);
            k1 *= C2_32;

            h1 ^= k1;
            h1 = Integer.rotateLeft(h1, 13);
            h1 = h1 * 5 + 0xe6546b64;
        }

        int k1 = 0;
        int tail = length & 0x03;
        if (tail == 3) {
            k1 ^= (data[roundedEnd + 2] & 0xff) << 16;
        }
        if (tail >= 2) {
            k1 ^= (data[roundedEnd + 1] & 0xff) << 8;
        }
        if (tail >= 1) {
            k1 ^= (data[roundedEnd] & 0xff);
            k1 *= C1_32;
            k1 = Integer.rotateLeft(k1, 15);
            k1 *= C2_32;
            h1 ^= k1;
        }

        h1 ^= length;
        h1 ^= h1 >>> 16;
        h1 *= 0x85ebca6b;
        h1 ^= h1 >>> 13;
        h1 *= 0xc2b2ae35;
        h1 ^= h1 >>> 16;

        return h1;
    }

    public static int hash32(byte[] data) {
        return hash32(data, 0, data.length, 0x9747b28c);
    }

    public static int hash32(String text) {
        byte[] bytes = text.getBytes(Constants.DEFAULT_CHARSET);
        return hash32(bytes, 0, bytes.length, 0x9747b28c);
    }

    /**
     * MurmurHash3 128-bit variant (returns pair of longs {h1, h2}).
     */
    public static long[] hash128(byte[] data, int offset, int length, long seed) {
        long h1 = seed;
        long h2 = seed;

        int nblocks = length / 16;

        for (int i = 0; i < nblocks; i++) {
            int index = offset + i * 16;
            long k1 = getLittleEndianLong(data, index);
            long k2 = getLittleEndianLong(data, index + 8);

            k1 *= C1_128;
            k1 = Long.rotateLeft(k1, 31);
            k1 *= C2_128;
            h1 ^= k1;

            h1 = Long.rotateLeft(h1, 27);
            h1 += h2;
            h1 = h1 * 5 + 0x52dce729;

            k2 *= C2_128;
            k2 = Long.rotateLeft(k2, 33);
            k2 *= C1_128;
            h2 ^= k2;

            h2 = Long.rotateLeft(h2, 31);
            h2 += h1;
            h2 = h2 * 5 + 0x38495ab5;
        }

        long k1 = 0;
        long k2 = 0;
        int tail = length % 16;
        int tailOffset = offset + nblocks * 16;

        switch (tail) {
            case 15: k2 ^= ((long) data[tailOffset + 14] & 0xff) << 48;
            case 14: k2 ^= ((long) data[tailOffset + 13] & 0xff) << 40;
            case 13: k2 ^= ((long) data[tailOffset + 12] & 0xff) << 32;
            case 12: k2 ^= ((long) data[tailOffset + 11] & 0xff) << 24;
            case 11: k2 ^= ((long) data[tailOffset + 10] & 0xff) << 16;
            case 10: k2 ^= ((long) data[tailOffset + 9] & 0xff) << 8;
            case 9:  k2 ^= ((long) data[tailOffset + 8] & 0xff);
                     k2 *= C2_128;
                     k2 = Long.rotateLeft(k2, 33);
                     k2 *= C1_128;
                     h2 ^= k2;
            case 8:  k1 ^= ((long) data[tailOffset + 7] & 0xff) << 56;
            case 7:  k1 ^= ((long) data[tailOffset + 6] & 0xff) << 48;
            case 6:  k1 ^= ((long) data[tailOffset + 5] & 0xff) << 40;
            case 5:  k1 ^= ((long) data[tailOffset + 4] & 0xff) << 32;
            case 4:  k1 ^= ((long) data[tailOffset + 3] & 0xff) << 24;
            case 3:  k1 ^= ((long) data[tailOffset + 2] & 0xff) << 16;
            case 2:  k1 ^= ((long) data[tailOffset + 1] & 0xff) << 8;
            case 1:  k1 ^= ((long) data[tailOffset] & 0xff);
                     k1 *= C1_128;
                     k1 = Long.rotateLeft(k1, 31);
                     k1 *= C2_128;
                     h1 ^= k1;
        }

        h1 ^= length;
        h2 ^= length;

        h1 += h2;
        h2 += h1;

        h1 = fmix64(h1);
        h2 = fmix64(h2);

        h1 += h2;
        h2 += h1;

        return new long[]{h1, h2};
    }

    private static long getLittleEndianLong(byte[] data, int index) {
        return (((long) data[index] & 0xff))
                | (((long) data[index + 1] & 0xff) << 8)
                | (((long) data[index + 2] & 0xff) << 16)
                | (((long) data[index + 3] & 0xff) << 24)
                | (((long) data[index + 4] & 0xff) << 32)
                | (((long) data[index + 5] & 0xff) << 40)
                | (((long) data[index + 6] & 0xff) << 48)
                | (((long) data[index + 7] & 0xff) << 56);
    }

    private static long fmix64(long k) {
        k ^= k >>> 33;
        k *= 0xff51afd7ed558ccdL;
        k ^= k >>> 33;
        k *= 0xc4ceb9fe1a85ec53L;
        k ^= k >>> 33;
        return k;
    }
}
