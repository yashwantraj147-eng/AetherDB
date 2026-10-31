package com.aetherdb.storage.sstable;

import com.aetherdb.common.BinaryUtils;
import com.aetherdb.model.DataRecord;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.zip.CRC32;

/**
 * Encapsulates an SSTable data block containing a sequence of sorted DataRecords.
 * Binary format:
 * [RecordCount (VarInt)][Record 1][Record 2]...[Record N][Block CRC32 (4B)]
 *
 * Enforces strict CRC32 verification on deserialization to detect data block corruption.
 */
public class DataBlock {

    public static final int MAX_RECORDS_PER_BLOCK = 1_000_000;

    private final List<DataRecord> records;
    private int estimatedBytes;

    public DataBlock() {
        this.records = new ArrayList<>();
        this.estimatedBytes = 4; // count overhead
    }

    public DataBlock(List<DataRecord> records) {
        this.records = new ArrayList<>(records);
        this.estimatedBytes = 4;
        for (DataRecord r : records) {
            this.estimatedBytes += r.estimatedSize();
        }
    }

    public void add(DataRecord record) {
        records.add(record);
        estimatedBytes += record.estimatedSize();
    }

    public List<DataRecord> getRecords() {
        return Collections.unmodifiableList(records);
    }

    public int getRecordCount() {
        return records.size();
    }

    public int getEstimatedBytes() {
        return estimatedBytes;
    }

    public byte[] getFirstKey() {
        return records.isEmpty() ? null : records.get(0).getKey().clone();
    }

    public byte[] getLastKey() {
        return records.isEmpty() ? null : records.get(records.size() - 1).getKey().clone();
    }

    /**
     * Serializes block to ByteBuffer including trailing CRC32.
     */
    public ByteBuffer serialize() {
        ByteBuffer buf = ByteBuffer.allocate(estimatedBytes + 64);
        BinaryUtils.writeVarInt(buf, records.size());
        for (DataRecord r : records) {
            r.serialize(buf);
        }
        int dataLen = buf.position();
        CRC32 crc = new CRC32();
        crc.update(buf.array(), 0, dataLen);
        long checksum = crc.getValue();
        buf.putInt((int) checksum);
        buf.flip();
        return buf;
    }

    /**
     * Deserializes block from ByteBuffer with mandatory CRC32 verification.
     */
    public static DataBlock deserialize(ByteBuffer buf) throws CorruptedSSTableException {
        if (buf.remaining() < 5) {
            throw new CorruptedSSTableException("DataBlock buffer too small: " + buf.remaining());
        }

        int startPos = buf.position();
        int recordCount = BinaryUtils.readVarInt(buf);
        if (recordCount < 0 || recordCount > MAX_RECORDS_PER_BLOCK) {
            throw new CorruptedSSTableException("Invalid DataBlock record count: " + recordCount);
        }

        List<DataRecord> list = new ArrayList<>(Math.min(recordCount, 1024));
        try {
            for (int i = 0; i < recordCount; i++) {
                list.add(DataRecord.deserialize(buf));
            }
        } catch (Exception e) {
            throw new CorruptedSSTableException("Failed to deserialize DataRecord in block: " + e.getMessage(), e);
        }

        int endDataPos = buf.position();
        if (buf.remaining() < 4) {
            throw new CorruptedSSTableException("Missing CRC32 in DataBlock");
        }
        int expectedCrc = buf.getInt();

        CRC32 crc = new CRC32();
        if (buf.hasArray()) {
            crc.update(buf.array(), buf.arrayOffset() + startPos, endDataPos - startPos);
        } else {
            ByteBuffer slice = buf.duplicate();
            slice.position(startPos);
            slice.limit(endDataPos);
            crc.update(slice);
        }
        long actualCrc = crc.getValue();

        if ((int) actualCrc != expectedCrc) {
            throw new CorruptedSSTableException("DataBlock CRC32 mismatch! Expected=" + expectedCrc + ", Actual=" + (int) actualCrc);
        }

        return new DataBlock(list);
    }
}
