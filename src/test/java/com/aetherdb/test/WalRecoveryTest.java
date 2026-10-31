package com.aetherdb.test;

import com.aetherdb.common.AetherConfig;
import com.aetherdb.common.Constants;
import com.aetherdb.model.DataRecord;
import com.aetherdb.storage.wal.CorruptedWalException;
import com.aetherdb.storage.wal.LogEntry;
import com.aetherdb.storage.wal.WriteAheadLog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class WalRecoveryTest {

    private Path tempWalDir;

    @BeforeEach
    void setUp() throws Exception {
        tempWalDir = Files.createTempDirectory("aether_wal_test_");
    }

    @AfterEach
    void tearDown() throws Exception {
        if (Files.exists(tempWalDir)) {
            Files.walk(tempWalDir)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        }
    }

    @Test
    @DisplayName("Normal append and recovery across multiple rotated WAL segments")
    void testMultiSegmentAppendAndRecovery() throws Exception {
        File wal1 = new File(tempWalDir.toFile(), WriteAheadLog.formatSegmentFilename(1));
        WriteAheadLog wal = new WriteAheadLog(wal1, AetherConfig.WalSyncMode.IMMEDIATE, 50);

        wal.append(DataRecord.put("k1", "v1", 101));
        wal.append(DataRecord.put("k2", "v2", 102));

        // Rotate to segment 2
        File rotated1 = wal.rotate(2);
        assertEquals(wal1.getAbsolutePath(), rotated1.getAbsolutePath());

        wal.append(DataRecord.put("k3", "v3", 103));
        wal.append(DataRecord.delete("k1", 104));

        wal.close();

        // Recover all segments
        List<DataRecord> recovered = WriteAheadLog.recoverAllSegments(tempWalDir.toFile());
        assertEquals(4, recovered.size());
        assertEquals("k1", recovered.get(0).getKeyAsString());
        assertEquals("v1", recovered.get(0).getValueAsString());
        assertEquals(101, recovered.get(0).getSequenceNumber());

        assertEquals("k2", recovered.get(1).getKeyAsString());
        assertEquals(102, recovered.get(1).getSequenceNumber());

        assertEquals("k3", recovered.get(2).getKeyAsString());
        assertEquals(103, recovered.get(2).getSequenceNumber());

        assertEquals("k1", recovered.get(3).getKeyAsString());
        assertTrue(recovered.get(3).isTombstone());
        assertEquals(104, recovered.get(3).getSequenceNumber());
    }

    @Test
    @DisplayName("Torn-tail handling: incomplete trailing record at EOF is truncated safely")
    void testTornTailTruncation() throws Exception {
        File walFile = new File(tempWalDir.toFile(), WriteAheadLog.formatSegmentFilename(1));
        WriteAheadLog wal = new WriteAheadLog(walFile, AetherConfig.WalSyncMode.IMMEDIATE, 50);

        wal.append(DataRecord.put("valid1", "data1", 1));
        wal.append(DataRecord.put("valid2", "data2", 2));
        wal.close();

        long validSize = walFile.length();
        assertTrue(validSize > 0);

        // Intentionally append partial corrupted bytes to simulate a power failure / crash mid-write
        try (RandomAccessFile raf = new RandomAccessFile(walFile, "rw")) {
            raf.seek(validSize);
            raf.writeInt(0x12345678); // Bogus CRC
            raf.writeInt(100);        // Claimed length 100 bytes
            raf.write(new byte[]{1, 2, 3, 4}); // Only 4 bytes written before crash
        }

        assertTrue(walFile.length() > validSize);

        // Recovery should detect torn tail, truncate the file to validSize, and recover the 2 valid records
        WriteAheadLog recoverWal = new WriteAheadLog(walFile, AetherConfig.WalSyncMode.IMMEDIATE, 50);
        List<DataRecord> records = recoverWal.recover();

        assertEquals(2, records.size());
        assertEquals("valid1", records.get(0).getKeyAsString());
        assertEquals("valid2", records.get(1).getKeyAsString());

        // Verify file was truncated back to valid size
        assertEquals(validSize, walFile.length());

        // Subsequent appends should work cleanly without hitting corruption
        recoverWal.append(DataRecord.put("valid3", "data3", 3));
        recoverWal.close();

        List<DataRecord> secondRecovery = WriteAheadLog.recoverAllSegments(tempWalDir.toFile());
        assertEquals(3, secondRecovery.size());
        assertEquals("valid3", secondRecovery.get(2).getKeyAsString());
    }

    @Test
    @DisplayName("Invalid magic bytes in WAL segment throws CorruptedWalException")
    void testInvalidMagicBytes() throws Exception {
        File walFile = new File(tempWalDir.toFile(), WriteAheadLog.formatSegmentFilename(1));
        try (RandomAccessFile raf = new RandomAccessFile(walFile, "rw")) {
            raf.writeInt(0xDEADBEEF); // Bad magic
            raf.writeShort(Constants.WAL_VERSION);
        }

        assertThrows(CorruptedWalException.class, () -> {
            WriteAheadLog.recoverFile(walFile, null, null);
        });
    }
}
