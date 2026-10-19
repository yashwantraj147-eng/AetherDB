package com.aetherdb.common;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.zip.CRC32;

/**
 * High-performance binary utility suite for serialization, variable-length integer encoding,
 * CRC32 checksumming, and lexicographical byte slice comparisons.
 */
public final class BinaryUtils {

    private BinaryUtils() {}

    /**
     * Lexicographical comparator for raw byte arrays.
     */
    public static int compareBytes(byte[] a, byte[] b) {
        return Arrays.compare(a, b);
    }

    /**
     * Lexicographical comparator for byte array slices.
     */
    public static int compareByteSlices(byte[] a, int aOffset, int aLen, byte[] b, int bOffset, int bLen) {
        int minLen = Math.min(aLen, bLen);
        for (int i = 0; i < minLen; i++) {
            int b1 = a[aOffset + i] & 0xFF;
            int b2 = b[bOffset + i] & 0xFF;
            if (b1 != b2) {
                return b1 - b2;
            }
        }
        return aLen - bLen;
    }

    /**
     * Writes a variable-length 32-bit integer (zigzag / LEB128 style).
     */
    public static void writeVarInt(ByteBuffer buffer, int value) {
        while ((value & ~0x7F) != 0) {
            buffer.put((byte) ((value & 0x7F) | 0x80));
            value >>>= 7;
        }
        buffer.put((byte) (value & 0x7F));
    }

    /**
     * Reads a variable-length 32-bit integer from a ByteBuffer.
     */
    public static int readVarInt(ByteBuffer buffer) {
        int value = 0;
        int shift = 0;
        byte b;
        do {
            b = buffer.get();
            value |= (b & 0x7F) << shift;
            shift += 7;
            if (shift > 35) {
                throw new IllegalArgumentException("Variable length integer overflow");
            }
        } while ((b & 0x80) != 0);
        return value;
    }

    /**
     * Writes a UTF-8 string prefixed by its length as a VarInt.
     */
    public static void writeString(ByteBuffer buffer, String str) {
        if (str == null) {
            writeVarInt(buffer, -1);
            return;
        }
        byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
        writeVarInt(buffer, bytes.length);
        buffer.put(bytes);
    }

    /**
     * Reads a length-prefixed UTF-8 string from a ByteBuffer.
     */
    public static String readString(ByteBuffer buffer) {
        int len = readVarInt(buffer);
        if (len == -1) return null;
        byte[] bytes = new byte[len];
        buffer.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    /**
     * Computes CRC32 checksum for a ByteBuffer slice without altering buffer position.
     */
    public static long computeChecksum(ByteBuffer buffer, int offset, int length) {
        CRC32 crc = new CRC32();
        ByteBuffer duplicate = buffer.duplicate();
        duplicate.position(offset);
        duplicate.limit(offset + length);
        crc.update(duplicate);
        return crc.getValue();
    }

    /**
     * Computes CRC32 checksum for a byte array.
     */
    public static long computeChecksum(byte[] data, int offset, int length) {
        CRC32 crc = new CRC32();
        crc.update(data, offset, length);
        return crc.getValue();
    }

    /**
     * Converts a hex string to byte array.
     */
    public static byte[] hexToBytes(String hex) {
        int len = hex.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(hex.charAt(i), 16) << 4)
                                 + Character.digit(hex.charAt(i + 1), 16));
        }
        return data;
    }

    /**
     * Converts a byte array to hex string.
     */
    public static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }
}
