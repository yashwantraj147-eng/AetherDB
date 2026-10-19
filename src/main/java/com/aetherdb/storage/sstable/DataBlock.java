package com.aetherdb.storage.sstable;

import com.aetherdb.common.BinaryUtils;
import com.aetherdb.model.DataRecord;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Encapsulates an SSTable data block containing a sequence of sorted DataRecords.
 * Binary format:
 * [RecordCount (VarInt)][Record 1][Record 2]...[Record N][Block CRC32 (4B)]
 */
public class DataBlock {

    private final List<DataRecord> records;
    private int estimatedBytes;

    public DataBlock() {
        this.records = new ArrayList<>();
        this.estimatedBytes = 4; // count overhead
    }

    public DataBlock(List<DataRecord> records) {
        this.records = records;
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
        return records;
    }

    public int getRecordCount() {
        return records.size();
    }

    public int getEstimatedBytes() {
        return estimatedBytes;
    }

    public byte[] getFirstKey() {
        return records.isEmpty() ? null : records.get(0).getKey();
    }

    public byte[] getLastKey() {
        return records.isEmpty() ? null : records.get(records.size() - 1).getKey();
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
        long crc = BinaryUtils.computeChecksum(buf.array(), 0, dataLen);
        buf.putInt((int) crc);
        buf.flip();
        return buf;
    }

    /**
     * Deserializes block from ByteBuffer.
     */
    public static DataBlock deserialize(ByteBuffer buf) {
        int recordCount = BinaryUtils.readVarInt(buf);
        List<DataRecord> list = new ArrayList<>(recordCount);
        for (int i = 0; i < recordCount; i++) {
            list.add(DataRecord.deserialize(buf));
        }
        int crc = buf.getInt(); // read checksum
        return new DataBlock(list);
    }
}
