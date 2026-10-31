package com.aetherdb.storage.sstable;

import com.aetherdb.common.BinaryUtils;
import com.aetherdb.common.BloomFilter;
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
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Reader for an immutable SSTable file.
 * Caches Metadata, Sparse Index, and Bloom Filter in memory for zero-I/O negative lookups
 * and single-I/O point reads.
 *
 * Implements reference counting to protect active readers from accessing closed or
 * prematurely deleted SSTable files during concurrent compactions.
 * Validates header magic, version, footer magic, non-overlapping regions, and record integrity.
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

            // 4. Validate non-overlapping regions and bounds
            long indexOffset = metadata.getIndexOffset();
            int indexLen = metadata.getIndexLength();
            long bloomOffset = metadata.getBloomOffset();
            int bloomLen = metadata.getBloomLength();

            if (indexOffset < 6 || indexLen < 0 || bloomOffset < 6 || bloomLen < 0) {
                throw new CorruptedSSTableException("Negative or invalid block offsets/lengths in " + sstableFile.getName());
            }

            if (indexOffset + (long) indexLen > bloomOffset) {
                throw new CorruptedSSTableException("IndexBlock overlaps with BloomFilter in " + sstableFile.getName());
            }

            if (bloomOffset + (long) bloomLen > footerOffset) {
                throw new CorruptedSSTableException("BloomFilter offset exceeds footer boundary in " + sstableFile.getName());
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

    public boolean isObsolete() {
        return obsolete;
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
        private Iterator<DataRecord> currentBlockIterator = null;
        private boolean iterClosed = false;

        public SSTableIterator() {
            advanceToNextBlock();
        }

        private void advanceToNextBlock() {
            while ((currentBlockIterator == null || !currentBlockIterator.hasNext())
                    && currentBlockIndex < indexBlock.size()) {
                try {
                    IndexBlock.IndexEntry entry = indexBlock.getEntry(currentBlockIndex++);
                    ByteBuffer blockBuf = ByteBuffer.allocate(entry.getLength());
                    readFully(channel, blockBuf, entry.getOffset());
                    blockBuf.flip();

                    DataBlock dataBlock = DataBlock.deserialize(blockBuf);
                    currentBlockIterator = dataBlock.getRecords().iterator();
                } catch (IOException e) {
                    throw new RuntimeException("Failed reading block in SSTable " + sstableFile.getName(), e);
                }
            }

            if ((currentBlockIterator == null || !currentBlockIterator.hasNext()) && !iterClosed) {
                iterClosed = true;
                releaseRef();
            }
        }

        @Override
        public boolean hasNext() {
            boolean has = currentBlockIterator != null && currentBlockIterator.hasNext();
            if (!has && !iterClosed) {
                iterClosed = true;
                releaseRef();
            }
            return has;
        }

        @Override
        public DataRecord next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            DataRecord rec = currentBlockIterator.next();
            if (!currentBlockIterator.hasNext()) {
                advanceToNextBlock();
            }
            return rec;
        }
    }

    @Override
    public void close() {
        if (closed) return;
        closed = true;
        releaseRef();
    }
}
