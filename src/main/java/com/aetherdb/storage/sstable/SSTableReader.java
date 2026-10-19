package com.aetherdb.storage.sstable;

import com.aetherdb.common.BinaryUtils;
import com.aetherdb.common.BloomFilter;
import com.aetherdb.model.DataRecord;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Reader for an immutable SSTable file.
 * Caches Metadata, Sparse Index, and Bloom Filter in memory for zero-I/O negative lookups
 * and single-I/O point reads.
 */
public class SSTableReader implements AutoCloseable, Iterable<DataRecord> {

    private final File sstableFile;
    private final FileChannel channel;
    private final SSTableMetadata metadata;
    private final IndexBlock indexBlock;
    private final BloomFilter bloomFilter;

    public SSTableReader(File sstableFile) throws IOException {
        this.sstableFile = sstableFile;
        this.channel = FileChannel.open(sstableFile.toPath(), StandardOpenOption.READ);

        // 1. Read trailing footer length
        long fileSize = channel.size();
        if (fileSize < 10) {
            throw new IllegalStateException("SSTable file too small: " + fileSize);
        }
        ByteBuffer lenBuf = ByteBuffer.allocate(4);
        channel.position(fileSize - 4);
        channel.read(lenBuf);
        lenBuf.flip();
        int footerLen = lenBuf.getInt();

        // 2. Read footer metadata
        ByteBuffer footerBuf = ByteBuffer.allocate(footerLen);
        channel.position(fileSize - 4 - footerLen);
        channel.read(footerBuf);
        footerBuf.flip();
        this.metadata = SSTableMetadata.deserialize(footerBuf);

        // 3. Read Index Block
        ByteBuffer indexBuf = ByteBuffer.allocate(metadata.getIndexLength());
        channel.position(metadata.getIndexOffset());
        channel.read(indexBuf);
        indexBuf.flip();
        this.indexBlock = IndexBlock.deserialize(indexBuf);

        // 4. Read Bloom Filter
        ByteBuffer bloomBuf = ByteBuffer.allocate(metadata.getBloomLength());
        channel.position(metadata.getBloomOffset());
        channel.read(bloomBuf);
        bloomBuf.flip();
        this.bloomFilter = BloomFilter.deserialize(bloomBuf);
    }

    public SSTableMetadata getMetadata() {
        return metadata;
    }

    public long getTableId() {
        return metadata.getTableId();
    }

    public int getLevel() {
        return metadata.getLevel();
    }

    public File getFile() {
        return sstableFile;
    }

    /**
     * Point lookup for a key:
     * 1. Min/Max key range check.
     * 2. In-memory Bloom Filter check.
     * 3. Sparse index binary search.
     * 4. Single-block disk read & record evaluation.
     */
    public DataRecord get(byte[] key) throws IOException {
        // Step 1: Range bounds check
        if (!metadata.couldContainKey(key)) {
            return null;
        }

        // Step 2: Bloom Filter negative check
        if (!bloomFilter.mightContain(key)) {
            return null;
        }

        // Step 3: Sparse index binary search
        int blockIdx = indexBlock.findBlockIndex(key);
        if (blockIdx < 0 || blockIdx >= indexBlock.size()) {
            return null;
        }

        // Step 4: Read targeted data block
        IndexBlock.IndexEntry entry = indexBlock.getEntry(blockIdx);
        ByteBuffer blockBuf = ByteBuffer.allocate(entry.getLength());
        synchronized (channel) {
            channel.position(entry.getOffset());
            channel.read(blockBuf);
        }
        blockBuf.flip();

        DataBlock dataBlock = DataBlock.deserialize(blockBuf);
        for (DataRecord record : dataBlock.getRecords()) {
            if (BinaryUtils.compareBytes(record.getKey(), key) == 0) {
                return record; // Records in block are ordered by seqNum descending
            }
        }

        return null;
    }

    @Override
    public Iterator<DataRecord> iterator() {
        return new SSTableIterator();
    }

    private class SSTableIterator implements Iterator<DataRecord> {
        private int currentBlockIndex = 0;
        private Iterator<DataRecord> currentRecordIterator = null;

        @Override
        public boolean hasNext() {
            while ((currentRecordIterator == null || !currentRecordIterator.hasNext())
                    && currentBlockIndex < indexBlock.size()) {
                try {
                    IndexBlock.IndexEntry entry = indexBlock.getEntry(currentBlockIndex++);
                    ByteBuffer blockBuf = ByteBuffer.allocate(entry.getLength());
                    synchronized (channel) {
                        channel.position(entry.getOffset());
                        channel.read(blockBuf);
                    }
                    blockBuf.flip();
                    DataBlock block = DataBlock.deserialize(blockBuf);
                    currentRecordIterator = block.getRecords().iterator();
                } catch (IOException e) {
                    throw new RuntimeException("Failed to read SSTable block at index " + (currentBlockIndex - 1), e);
                }
            }
            return currentRecordIterator != null && currentRecordIterator.hasNext();
        }

        @Override
        public DataRecord next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return currentRecordIterator.next();
        }
    }

    @Override
    public void close() throws IOException {
        if (channel.isOpen()) {
            channel.close();
        }
    }
}
