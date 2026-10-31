package com.aetherdb.storage.sstable;

import com.aetherdb.common.BinaryUtils;
import com.aetherdb.common.BloomFilter;
import com.aetherdb.model.DataRecord;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Reader for an immutable SSTable file.
 * Caches Metadata, Sparse Index, and Bloom Filter in memory for zero-I/O negative lookups
 * and single-I/O point reads.
 *
 * Implements reference counting to protect active readers from accessing closed or
 * prematurely deleted SSTable files during concurrent compactions.
 */
public class SSTableReader implements AutoCloseable, Iterable<DataRecord> {

    public static final int MAX_FOOTER_SIZE = 65536;

    private final File sstableFile;
    private final FileChannel channel;
    private final SSTableMetadata metadata;
    private final IndexBlock indexBlock;
    private final BloomFilter bloomFilter;

    private final AtomicInteger refCount = new AtomicInteger(1);
    private volatile boolean obsolete = false;
    private volatile boolean closed = false;

    public SSTableReader(File sstableFile) throws IOException {
        this.sstableFile = sstableFile;
        this.channel = FileChannel.open(sstableFile.toPath(), StandardOpenOption.READ);

        long fileSize = channel.size();
        if (fileSize < 10) {
            channel.close();
            throw new CorruptedSSTableException("SSTable file too small: " + fileSize + " in " + sstableFile.getName());
        }

        // 1. Read trailing footer length
        ByteBuffer lenBuf = ByteBuffer.allocate(4);
        readFully(channel, lenBuf, fileSize - 4);
        lenBuf.flip();
        int footerLen = lenBuf.getInt();

        if (footerLen <= 0 || footerLen > MAX_FOOTER_SIZE || footerLen > fileSize - 10) {
            channel.close();
            throw new CorruptedSSTableException("Invalid SSTable footer length: " + footerLen + " in " + sstableFile.getName());
        }

        // 2. Read footer metadata
        ByteBuffer footerBuf = ByteBuffer.allocate(footerLen);
        readFully(channel, footerBuf, fileSize - 4 - footerLen);
        footerBuf.flip();
        try {
            this.metadata = SSTableMetadata.deserialize(footerBuf);
        } catch (Exception e) {
            channel.close();
            throw new CorruptedSSTableException("Failed to parse SSTable metadata in " + sstableFile.getName() + ": " + e.getMessage(), e);
        }

        // Validate block offsets against file boundary
        long maxPayloadBoundary = fileSize - 4 - footerLen;
        if (metadata.getIndexOffset() + metadata.getIndexLength() > maxPayloadBoundary) {
            channel.close();
            throw new CorruptedSSTableException("IndexBlock offset exceeds file bounds in " + sstableFile.getName());
        }
        if (metadata.getBloomOffset() + metadata.getBloomLength() > maxPayloadBoundary) {
            channel.close();
            throw new CorruptedSSTableException("BloomFilter offset exceeds file bounds in " + sstableFile.getName());
        }

        // 3. Read Index Block
        ByteBuffer indexBuf = ByteBuffer.allocate(metadata.getIndexLength());
        readFully(channel, indexBuf, metadata.getIndexOffset());
        indexBuf.flip();
        try {
            this.indexBlock = IndexBlock.deserialize(indexBuf);
        } catch (Exception e) {
            channel.close();
            throw new CorruptedSSTableException("Failed to parse IndexBlock in " + sstableFile.getName() + ": " + e.getMessage(), e);
        }

        // 4. Read Bloom Filter
        ByteBuffer bloomBuf = ByteBuffer.allocate(metadata.getBloomLength());
        readFully(channel, bloomBuf, metadata.getBloomOffset());
        bloomBuf.flip();
        try {
            this.bloomFilter = BloomFilter.deserialize(bloomBuf);
        } catch (Exception e) {
            channel.close();
            throw new CorruptedSSTableException("Failed to parse BloomFilter in " + sstableFile.getName() + ": " + e.getMessage(), e);
        }
    }

    private static void readFully(FileChannel ch, ByteBuffer buf, long position) throws IOException {
        long curPos = position;
        while (buf.hasRemaining()) {
            int read = ch.read(buf, curPos);
            if (read < 0) {
                throw new EOFException("Premature EOF reading SSTable at position " + curPos);
            }
            curPos += read;
        }
    }

    /**
     * Acquires a reader reference. Returns false if the reader is already closed.
     */
    public boolean acquireRef() {
        while (true) {
            int current = refCount.get();
            if (current <= 0 || closed) {
                return false;
            }
            if (refCount.compareAndSet(current, current + 1)) {
                return true;
            }
        }
    }

    /**
     * Releases a reader reference. If refCount reaches zero and marked obsolete, cleans up.
     */
    public void releaseRef() {
        int remaining = refCount.decrementAndGet();
        if (remaining == 0) {
            cleanup();
        }
    }

    /**
     * Marks this SSTable as obsolete (superseded by compaction).
     */
    public void markObsolete() {
        this.obsolete = true;
        close();
    }

    private void cleanup() {
        try {
            if (channel.isOpen()) {
                channel.close();
            }
        } catch (IOException ignored) {}

        if (obsolete && sstableFile.exists()) {
            sstableFile.delete();
        }
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

    public boolean isClosed() {
        return closed;
    }

    /**
     * Point lookup for a key:
     * 1. Min/Max key range check.
     * 2. In-memory Bloom Filter check.
     * 3. Sparse index binary search.
     * 4. Single-block disk read & record evaluation.
     */
    public DataRecord get(byte[] key) throws IOException {
        if (closed || !acquireRef()) {
            return null;
        }
        try {
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
            readFully(channel, blockBuf, entry.getOffset());
            blockBuf.flip();

            DataBlock dataBlock = DataBlock.deserialize(blockBuf);
            DataRecord newestMatch = null;
            for (DataRecord record : dataBlock.getRecords()) {
                if (BinaryUtils.compareBytes(record.getKey(), key) == 0) {
                    if (newestMatch == null || record.getSequenceNumber() > newestMatch.getSequenceNumber()) {
                        newestMatch = record;
                    }
                }
            }

            return newestMatch;
        } finally {
            releaseRef();
        }
    }

    @Override
    public Iterator<DataRecord> iterator() {
        if (!acquireRef()) {
            throw new IllegalStateException("Cannot iterate over closed SSTable: " + sstableFile.getName());
        }
        return new SSTableIterator();
    }

    private class SSTableIterator implements Iterator<DataRecord> {
        private int currentBlockIndex = 0;
        private Iterator<DataRecord> currentRecordIterator = null;
        private boolean iterClosed = false;

        @Override
        public boolean hasNext() {
            if (iterClosed) return false;
            while ((currentRecordIterator == null || !currentRecordIterator.hasNext())
                    && currentBlockIndex < indexBlock.size()) {
                try {
                    IndexBlock.IndexEntry entry = indexBlock.getEntry(currentBlockIndex++);
                    ByteBuffer blockBuf = ByteBuffer.allocate(entry.getLength());
                    readFully(channel, blockBuf, entry.getOffset());
                    blockBuf.flip();
                    DataBlock block = DataBlock.deserialize(blockBuf);
                    currentRecordIterator = block.getRecords().iterator();
                } catch (IOException e) {
                    closeIterator();
                    throw new RuntimeException("Failed to read SSTable block at index " + (currentBlockIndex - 1)
                            + " in " + sstableFile.getName(), e);
                }
            }
            boolean has = currentRecordIterator != null && currentRecordIterator.hasNext();
            if (!has) {
                closeIterator();
            }
            return has;
        }

        @Override
        public DataRecord next() {
            if (!hasNext()) {
                closeIterator();
                throw new NoSuchElementException();
            }
            return currentRecordIterator.next();
        }

        private void closeIterator() {
            if (!iterClosed) {
                iterClosed = true;
                releaseRef();
            }
        }
    }

    @Override
    public void close() {
        if (closed) return;
        closed = true;
        releaseRef();
    }
}
