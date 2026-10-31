package com.aetherdb.storage.sstable;

import com.aetherdb.common.BinaryUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Sparse binary search index for SSTable data blocks.
 * Maps block boundary keys to file byte offsets and lengths.
 */
public class IndexBlock {

    public static class IndexEntry {
        private final byte[] lastKey;
        private final long offset;
        private final int length;

        public IndexEntry(byte[] lastKey, long offset, int length) {
            this.lastKey = (lastKey != null) ? lastKey.clone() : new byte[0];
            this.offset = offset;
            this.length = length;
        }

        public byte[] getLastKey() { return lastKey.clone(); }
        public long getOffset() { return offset; }
        public int getLength() { return length; }
    }

    private final List<IndexEntry> entries;

    public IndexBlock() {
        this.entries = new ArrayList<>();
    }

    public IndexBlock(List<IndexEntry> entries) {
        this.entries = new ArrayList<>(entries);
    }

    public void addEntry(byte[] lastKey, long offset, int length) {
        entries.add(new IndexEntry(lastKey, offset, length));
    }

    public List<IndexEntry> getEntries() {
        return Collections.unmodifiableList(entries);
    }

    public int size() {
        return entries.size();
    }

    /**
     * Binary search to find the index entry whose block could contain the key.
     * Returns index of the candidate entry, or -1 if the key is greater than all blocks.
     */
    public int findBlockIndex(byte[] key) {
        if (key == null || entries.isEmpty()) return -1;

        int low = 0;
        int high = entries.size() - 1;
        int result = -1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            IndexEntry entry = entries.get(mid);
            int cmp = BinaryUtils.compareBytes(key, entry.lastKey);

            if (cmp <= 0) {
                result = mid;
                high = mid - 1; // Search left for the first block whose lastKey >= key
            } else {
                low = mid + 1;
            }
        }

        return result;
    }

    public IndexEntry getEntry(int index) {
        return entries.get(index);
    }

    /**
     * Serializes the index block to ByteBuffer.
     */
    public ByteBuffer serialize() {
        int estimatedSize = 8;
        for (IndexEntry e : entries) {
            estimatedSize += 8 + e.lastKey.length + 8 + 4;
        }
        ByteBuffer buf = ByteBuffer.allocate(estimatedSize);
        BinaryUtils.writeVarInt(buf, entries.size());
        for (IndexEntry e : entries) {
            BinaryUtils.writeVarInt(buf, e.lastKey.length);
            buf.put(e.lastKey);
            buf.putLong(e.offset);
            buf.putInt(e.length);
        }
        buf.flip();
        return buf;
    }

    /**
     * Deserializes index block from ByteBuffer with bounds validation.
     */
    public static IndexBlock deserialize(ByteBuffer buf) throws CorruptedSSTableException {
        if (buf.remaining() < 1) {
            throw new CorruptedSSTableException("IndexBlock buffer empty");
        }
        int count = BinaryUtils.readVarInt(buf);
        if (count < 0 || count > 10_000_000) {
            throw new CorruptedSSTableException("Invalid IndexBlock entry count: " + count);
        }

        List<IndexEntry> list = new ArrayList<>(Math.min(count, 1024));
        for (int i = 0; i < count; i++) {
            int keyLen = BinaryUtils.readVarInt(buf);
            if (keyLen < 0 || keyLen > 65536 || buf.remaining() < keyLen) {
                throw new CorruptedSSTableException("Corrupt key length in index entry at index " + i + ": " + keyLen);
            }
            byte[] key = new byte[keyLen];
            buf.get(key);
            if (buf.remaining() < 12) {
                throw new CorruptedSSTableException("Missing offset or length in index entry at index " + i);
            }
            long offset = buf.getLong();
            int length = buf.getInt();
            if (offset < 0 || length <= 0) {
                throw new CorruptedSSTableException("Invalid offset/length in index entry at index " + i);
            }
            list.add(new IndexEntry(key, offset, length));
        }
        return new IndexBlock(list);
    }
}
