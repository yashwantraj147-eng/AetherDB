package com.aetherdb.common;

import java.nio.ByteBuffer;
import java.util.BitSet;

/**
 * Production Bloom Filter implementation with Kirsch-Mitzenmacher double-hashing optimization.
 * Formula: g_i(x) = h1(x) + i * h2(x) (mod m)
 * Provides optimal bit allocation and number of hash functions based on target false positive rate.
 */
public class BloomFilter {

    private final int numBits;
    private final int numHashFunctions;
    private final BitSet bitSet;

    public BloomFilter(int expectedInsertions, double falsePositiveRate) {
        if (expectedInsertions <= 0) expectedInsertions = 1;
        if (falsePositiveRate <= 0.0 || falsePositiveRate >= 1.0) {
            falsePositiveRate = Constants.DEFAULT_BLOOM_FPR;
        }

        // Optimal bit count: m = - (n * ln(p)) / (ln(2)^2)
        this.numBits = (int) Math.ceil(-1.0 * expectedInsertions * Math.log(falsePositiveRate) / (Math.log(2) * Math.log(2)));
        // Optimal hash count: k = (m / n) * ln(2)
        int k = (int) Math.round(((double) this.numBits / expectedInsertions) * Math.log(2));
        this.numHashFunctions = Math.max(1, k);
        this.bitSet = new BitSet(numBits);
    }

    private BloomFilter(int numBits, int numHashFunctions, BitSet bitSet) {
        this.numBits = numBits;
        this.numHashFunctions = numHashFunctions;
        this.bitSet = bitSet;
    }

    /**
     * Inserts raw byte key into the filter.
     */
    public void add(byte[] key) {
        long[] hashes = MurmurHash3.hash128(key, 0, key.length, 0x12345678L);
        long h1 = hashes[0];
        long h2 = hashes[1];

        for (int i = 0; i < numHashFunctions; i++) {
            long combinedHash = h1 + i * h2;
            int bitIndex = (int) Math.floorMod(combinedHash, (long) numBits);
            bitSet.set(bitIndex);
        }
    }

    public void add(String key) {
        add(key.getBytes(Constants.DEFAULT_CHARSET));
    }

    /**
     * Tests if key might be in the set (zero false negatives, bounded false positives).
     */
    public boolean mightContain(byte[] key) {
        long[] hashes = MurmurHash3.hash128(key, 0, key.length, 0x12345678L);
        long h1 = hashes[0];
        long h2 = hashes[1];

        for (int i = 0; i < numHashFunctions; i++) {
            long combinedHash = h1 + i * h2;
            int bitIndex = (int) Math.floorMod(combinedHash, (long) numBits);
            if (!bitSet.get(bitIndex)) {
                return false;
            }
        }
        return true;
    }

    public boolean mightContain(String key) {
        return mightContain(key.getBytes(Constants.DEFAULT_CHARSET));
    }

    public int getNumBits() {
        return numBits;
    }

    public int getNumHashFunctions() {
        return numHashFunctions;
    }

    /**
     * Serializes the bloom filter to raw binary format for SSTable storage.
     */
    public byte[] serialize() {
        byte[] rawBits = bitSet.toByteArray();
        ByteBuffer buffer = ByteBuffer.allocate(8 + 4 + 4 + rawBits.length);
        buffer.putInt(numBits);
        buffer.putInt(numHashFunctions);
        buffer.putInt(rawBits.length);
        buffer.put(rawBits);
        return buffer.array();
    }

    /**
     * Deserializes a bloom filter from a ByteBuffer.
     */
    public static BloomFilter deserialize(ByteBuffer buffer) {
        int numBits = buffer.getInt();
        int numHashFunctions = buffer.getInt();
        int rawBitsLen = buffer.getInt();
        byte[] rawBits = new byte[rawBitsLen];
        buffer.get(rawBits);
        BitSet bitSet = BitSet.valueOf(rawBits);
        return new BloomFilter(numBits, numHashFunctions, bitSet);
    }
}
