package com.aetherdb.storage.wal;

import com.aetherdb.common.BinaryUtils;
import com.aetherdb.model.DataRecord;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

/**
 * Encapsulates a single framed binary record in the Write-Ahead Log.
 * Binary format:
 * [CRC32 Checksum (4B)][Payload Length (4B)][Serialized DataRecord]
 */
public class LogEntry {

    private final long crc32;
    private final DataRecord record;

    public LogEntry(DataRecord record) {
        this.record = record;
        this.crc32 = computeCrc(record);
    }

    public LogEntry(long crc32, DataRecord record) {
        this.crc32 = crc32;
        this.record = record;
    }

    public long getCrc32() {
        return crc32;
    }

    public DataRecord getRecord() {
        return record;
    }

    private static long computeCrc(DataRecord record) {
        int estimatedSize = record.estimatedSize() + 32;
        ByteBuffer buf = ByteBuffer.allocate(estimatedSize);
        record.serialize(buf);
        buf.flip();
        CRC32 crc = new CRC32();
        crc.update(buf.array(), 0, buf.remaining());
        return crc.getValue();
    }

    /**
     * Serializes this WAL frame to a target ByteBuffer.
     */
    public void serialize(ByteBuffer buffer) {
        int estimated = record.estimatedSize() + 32;
        ByteBuffer payloadBuf = ByteBuffer.allocate(estimated);
        record.serialize(payloadBuf);
        payloadBuf.flip();
        int payloadLen = payloadBuf.remaining();

        buffer.putInt((int) crc32);
        buffer.putInt(payloadLen);
        buffer.put(payloadBuf);
    }

    /**
     * Reads and validates a WAL frame from a ByteBuffer.
     * Returns null if not enough bytes are available for the header.
     */
    public static LogEntry deserialize(ByteBuffer buffer) {
        if (buffer.remaining() < 8) {
            return null;
        }

        int startPos = buffer.position();
        int expectedCrc = buffer.getInt();
        int payloadLen = buffer.getInt();

        if (payloadLen <= 0 || payloadLen > 128 * 1024 * 1024) {
            throw new IllegalStateException("Corrupt WAL frame payload length: " + payloadLen);
        }

        if (buffer.remaining() < payloadLen) {
            buffer.position(startPos);
            return null; // Partial frame at end of file
        }

        int payloadOffset = buffer.position();
        byte[] payloadBytes = new byte[payloadLen];
        buffer.get(payloadBytes);

        CRC32 crc = new CRC32();
        crc.update(payloadBytes);
        long actualCrc = crc.getValue();

        if ((int) actualCrc != expectedCrc) {
            throw new IllegalStateException("WAL CRC32 mismatch! Expected=" + expectedCrc + ", Actual=" + (int) actualCrc);
        }

        ByteBuffer payloadBuffer = ByteBuffer.wrap(payloadBytes);
        DataRecord record = DataRecord.deserialize(payloadBuffer);
        return new LogEntry(actualCrc, record);
    }
}
