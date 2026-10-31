package com.aetherdb.storage.sstable;

import com.aetherdb.common.BinaryUtils;
import com.aetherdb.common.BloomFilter;
import com.aetherdb.common.CloseableIterator;
import com.aetherdb.common.Constants;
import com.aetherdb.model.DataRecord;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Reader for an immutable SSTable file.
 * Caches Metadata, Sparse Index, and Bloom Filter in memory for zero-I/O negative lookups
 * and single-I/O point reads.
 *
 * Implements reference counting to protect active readers from accessing closed or
 * prematurely deleted SSTable files during concurrent compactions.
 * Validates header magic, version, footer magic, overflow-safe non-overlapping regions, and record integrity.
 */
public class SSTableReader implements AutoCloseable, Iterable<DataRecord> {

    public static final int MAX_FOOTER_SIZE = 65536;
    public static final int MAX_INDEX_SIZE = 64 * 1024 * 1024; // 64 MB
    public static final int MAX_BLOOM_SIZE = 64 * 1024 * 1024; // 64 MB

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

        try {
            long fileSize = channel.size();
            if (fileSize < 16) {
                throw new CorruptedSSTableException("SSTable file too small: " + fileSize + " bytes in " + sstableFile.getName());
            }

            // 1. Read and validate 6-byte header magic and version
            ByteBuffer headerBuf = ByteBuffer.allocate(6);
            readFully(channel, headerBuf, 0);
            headerBuf.flip();
            int headerMagic = headerBuf.getInt();
            short headerVersion = headerBuf.getShort();
            if (headerMagic != Constants.SSTABLE_MAGIC) {
                throw new CorruptedSSTableException("Invalid SSTable header magic bytes: 0x"
                        + Integer.toHexString(headerMagic) + " in " + sstableFile.getName());
            }
            if (headerVersion != Constants.SSTABLE_VERSION) {
                throw new CorruptedSSTableException("Unsupported SSTable header version: "
                        + headerVersion + " in " + sstableFile.getName());
            }

            // 2. Read trailing footer length
            ByteBuffer lenBuf = ByteBuffer.allocate(4);
            readFully(channel, lenBuf, fileSize - 4);
            lenBuf.flip();
            int footerLen = lenBuf.getInt();

            if (footerLen <= 0 || footerLen > MAX_FOOTER_SIZE || footerLen > fileSize - 10) {
                throw new CorruptedSSTableException("Invalid SSTable footer length: " + footerLen + " in " + sstableFile.getName());
            }

            long footerOffset = fileSize - 4 - footerLen;

            // 3. Read footer metadata
            ByteBuffer footerBuf = ByteBuffer.allocate(footerLen);
            readFully(channel, footerBuf, footerOffset);
            footerBuf.flip();
            try {
                this.metadata = SSTableMetadata.deserialize(footerBuf);
            } catch (Exception e) {
                throw new CorruptedSSTableException("Failed to parse SSTable metadata in " + sstableFile.getName() + ": " + e.getMessage(), e);
            }

            // 4. Validate non-overlapping regions and bounds with overflow-safe arithmetic
            long indexOffset = metadata.getIndexOffset();
            int indexLen = metadata.getIndexLength();
            long bloomOffset = metadata.getBloomOffset();
            int bloomLen = metadata.getBloomLength();

            if (indexOffset < 6 || indexLen <= 0 || bloomOffset < 6 || bloomLen <= 0) {
                throw new CorruptedSSTableException("Negative or invalid block offsets/lengths in " + sstableFile.getName());
            }

            if (indexLen > MAX_INDEX_SIZE || bloomLen > MAX_BLOOM_SIZE) {
                throw new CorruptedSSTableException("Block size exceeds maximum allowed allocation in " + sstableFile.getName());
            }

            try {
                long indexEnd = Math.addExact(indexOffset, (long) indexLen);
                if (indexEnd > bloomOffset) {
                    throw new CorruptedSSTableException("IndexBlock overlaps with BloomFilter in " + sstableFile.getName());
                }

                long bloomEnd = Math.addExact(bloomOffset, (long) bloomLen);
                if (bloomEnd > footerOffset) {
                    throw new CorruptedSSTableException("BloomFilter offset exceeds footer boundary in " + sstableFile.getName());
                }

                long footerEnd = Math.addExact(footerOffset, Math.addExact((long) footerLen, 4L));
                if (footerEnd > fileSize) {
                    throw new CorruptedSSTableException("Footer region exceeds total file size in " + sstableFile.getName());
                }
            } catch (ArithmeticException e) {
                throw new CorruptedSSTableException("Integer overflow in SSTable offset calculation in " + sstableFile.getName(), e);
            }

            // 5. Read Index Block
            ByteBuffer indexBuf = ByteBuffer.allocate(indexLen);
            readFully(channel, indexBuf, indexOffset);
            indexBuf.flip();
            try {
                this.indexBlock = IndexBlock.deserialize(indexBuf);
            } catch (Exception e) {
                throw new CorruptedSSTableException("Failed to parse IndexBlock in " + sstableFile.getName() + ": " + e.getMessage(), e);
            }

            // 6. Read Bloom Filter
            ByteBuffer bloomBuf = ByteBuffer.allocate(bloomLen);
            readFully(channel, bloomBuf, bloomOffset);
            bloomBuf.flip();
            try {
                this.bloomFilter = BloomFilter.deserialize(bloomBuf);
            } catch (Exception e) {
                throw new CorruptedSSTableException("Failed to parse BloomFilter in " + sstableFile.getName() + ": " + e.getMessage(), e);
            }
        } catch (Throwable t) {
            try {
                channel.close();
            } catch (IOException ignored) {}
            if (t instanceof IOException) {
                throw (IOException) t;
            } else {
                throw new CorruptedSSTableException("Corrupted SSTable file: " + sstableFile.getName() + " - " + t.getMessage(), t);
            }
        }
    }

    private static void readFully(FileChannel ch, ByteBuffer buf, long position) throws IOException {
        long curPos = position;
        while (buf.hasRemaining()) {
            int read = ch.read(buf, curPos);
            if (read == -1) {
                throw new EOFException("Premature EOF reading SSTable at position: " + curPos);
            }
            curPos += read;
        }
    }

    public long getTableId() {
        return metadata.getTableId();
    }

    public SSTableMetadata getMetadata() {
        return metadata;
    }

    public IndexBlock getIndexBlock() {
        return indexBlock;
    }

    public BloomFilter getBloomFilter() {
        return bloomFilter;
    }

    public File getFile() {
        return sstableFile;
    }

    public int getRefCount() {
        return refCount.get();
    }

    public boolean isClosed() {
        return closed;
    }

    public boolean isObsolete() {
        return obsolete;
    }

    /**
     * Atomically acquires a reader reference.
     * Returns false if the reader has already been closed.
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
     * Releases a reader reference. If reference count reaches zero and the table
     * was marked obsolete or closed, closes channel and physically deletes files.
     */
    public void releaseRef() {
        int remaining = refCount.decrementAndGet();
        if (remaining == 0) {
            try {
                if (channel != null && channel.isOpen()) {
                    channel.close();
                }
            } catch (IOException ignored) {}

            if (obsolete && sstableFile.exists()) {
                sstableFile.delete();
            }
        }
    }

    /**
     * Marks this SSTable as obsolete (superseded by a newly compacted SSTable).
     * Defers channel closing and physical file deletion until all active readers release references.
     */
    public void markObsolete() {
        this.obsolete = true;
        releaseRef();
    }

    /**
     * Point lookup for a key in this SSTable.
     * Uses BloomFilter $\to$ IndexBlock $\to$ DataBlock single-I/O traversal.
     */
    public DataRecord get(byte[] key) throws IOException {
        if (!acquireRef()) {
            return null;
        }

        try {
            // Step 1: Min-Max key bounds check
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
    public CloseableIterator<DataRecord> iterator() {
        if (!acquireRef()) {
            throw new IllegalStateException("Cannot iterate over closed SSTable: " + sstableFile.getName());
        }
        return new SSTableIterator();
    }

    private class SSTableIterator implements CloseableIterator<DataRecord> {
        private int currentBlockIndex = 0;
        private Iterator<DataRecord> currentBlockIterator = null;
        private final AtomicBoolean isClosed = new AtomicBoolean(false);

        public SSTableIterator() {
            advanceToNextBlock();
        }

        private void advanceToNextBlock() {
            if (isClosed.get()) return;
            while ((currentBlockIterator == null || !currentBlockIterator.hasNext())
                    && currentBlockIndex < indexBlock.size()) {
                try {
                    IndexBlock.IndexEntry entry = indexBlock.getEntry(currentBlockIndex++);
                    ByteBuffer blockBuf = ByteBuffer.allocate(entry.getLength());
                    readFully(channel, blockBuf, entry.getOffset());
                    blockBuf.flip();

                    DataBlock dataBlock = DataBlock.deserialize(blockBuf);
                    currentBlockIterator = dataBlock.getRecords().iterator();
                } catch (Throwable t) {
                    close();
                    if (t instanceof RuntimeException re) throw re;
                    throw new RuntimeException("Failed reading block in SSTable " + sstableFile.getName(), t);
                }
            }

            if ((currentBlockIterator == null || !currentBlockIterator.hasNext())) {
                close();
            }
        }

        @Override
        public boolean hasNext() {
            if (isClosed.get()) return false;
            boolean has = currentBlockIterator != null && currentBlockIterator.hasNext();
            if (!has) {
                close();
            }
            return has;
        }

        @Override
        public DataRecord next() {
            if (isClosed.get() || !hasNext()) {
                throw new NoSuchElementException();
            }
            DataRecord rec = currentBlockIterator.next();
            if (!currentBlockIterator.hasNext()) {
                advanceToNextBlock();
            }
            return rec;
        }

        @Override
        public void close() {
            if (isClosed.compareAndSet(false, true)) {
                currentBlockIterator = null;
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
