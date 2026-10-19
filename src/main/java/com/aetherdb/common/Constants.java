package com.aetherdb.common;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * Global constants for AetherDB storage formats, magic bytes, versions,
 * protocol definitions, and default thresholds.
 */
public final class Constants {

    private Constants() {}

    // Magic Numbers for File Formats
    public static final int WAL_MAGIC = 0x4145574C;      // "AEWL" (Aether Write-Ahead Log)
    public static final int SSTABLE_MAGIC = 0x41455353;  // "AESS" (Aether Sorted String Table)
    public static final int COLUMNAR_MAGIC = 0x4145434F; // "AECO" (Aether Columnar Format)

    // Format Versioning
    public static final short WAL_VERSION = 1;
    public static final short SSTABLE_VERSION = 1;
    public static final short COLUMNAR_VERSION = 1;

    // Charset
    public static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;

    // Tombstone marker for deleted records
    public static final byte[] TOMBSTONE_PAYLOAD = new byte[0];

    // File Extensions
    public static final String WAL_FILE_EXT = ".wal";
    public static final String SSTABLE_DATA_EXT = ".sst";
    public static final String SSTABLE_INDEX_EXT = ".idx";
    public static final String SSTABLE_BLOOM_EXT = ".blm";
    public static final String COLUMNAR_CHUNK_EXT = ".achunk";

    // Defaults
    public static final int DEFAULT_PORT = 8080;
    public static final int DEFAULT_BLOCK_SIZE = 64 * 1024; // 64 KB
    public static final long DEFAULT_MEMTABLE_SIZE = 32 * 1024 * 1024; // 32 MB
    public static final double DEFAULT_BLOOM_FPR = 0.01; // 1% false positive probability
    public static final int DEFAULT_VNODES = 64;
}
