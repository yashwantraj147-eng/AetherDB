package com.aetherdb.storage.sstable;

import com.aetherdb.common.BloomFilter;
import com.aetherdb.common.Constants;
import com.aetherdb.model.DataRecord;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;

/**
 * Sequential builder that flushes sorted DataRecords into an immutable SSTable file.
 * Structure:
 * [File Header] -> [Data Blocks 0..N] -> [Index Block] -> [Bloom Filter] -> [Footer] -> [Footer Length (4B)]
 */
public class SSTableWriter {

    private final File sstableFile;
    private final long tableId;
    private final int level;
    private final int targetBlockSize;
    private final double bloomFpr;

    public SSTableWriter(File sstableFile, long tableId, int level, int targetBlockSize, double bloomFpr) {
        this.sstableFile = sstableFile;
        this.tableId = tableId;
        this.level = level;
        this.targetBlockSize = targetBlockSize;
        this.bloomFpr = bloomFpr;
    }

    /**
     * Writes all records from the iterator into the SSTable file and returns its metadata.
     */
    public SSTableMetadata write(Iterator<DataRecord> iterator, int estimatedCount) throws IOException {
        BloomFilter bloomFilter = new BloomFilter(Math.max(estimatedCount, 100), bloomFpr);
        IndexBlock indexBlock = new IndexBlock();

        byte[] minKey = null;
        byte[] maxKey = null;
        long minSeq = Long.MAX_VALUE;
        long maxSeq = Long.MIN_VALUE;
        long recordCount = 0;

        try (FileChannel channel = FileChannel.open(sstableFile.toPath(),
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING)) {

            // 1. Write Header
            ByteBuffer header = ByteBuffer.allocate(6);
            header.putInt(Constants.SSTABLE_MAGIC);
            header.putShort(Constants.SSTABLE_VERSION);
            header.flip();
            channel.write(header);

            // 2. Write Data Blocks
            DataBlock currentBlock = new DataBlock();

            while (iterator.hasNext()) {
                DataRecord record = iterator.next();
                recordCount++;

                if (minKey == null) {
                    minKey = record.getKey().clone();
                }
                maxKey = record.getKey().clone();

                long seq = record.getSequenceNumber();
                if (seq < minSeq) minSeq = seq;
                if (seq > maxSeq) maxSeq = seq;

                bloomFilter.add(record.getKey());
                currentBlock.add(record);

                if (currentBlock.getEstimatedBytes() >= targetBlockSize) {
                    flushDataBlock(channel, currentBlock, indexBlock);
                    currentBlock = new DataBlock();
                }
            }

            if (currentBlock.getRecordCount() > 0) {
                flushDataBlock(channel, currentBlock, indexBlock);
            }

            long dataSize = channel.position();

            // 3. Write Index Block
            long indexOffset = channel.position();
            ByteBuffer indexBuf = indexBlock.serialize();
            int indexLen = indexBuf.remaining();
            channel.write(indexBuf);

            // 4. Write Bloom Filter
            long bloomOffset = channel.position();
            byte[] bloomBytes = bloomFilter.serialize();
            ByteBuffer bloomBuf = ByteBuffer.wrap(bloomBytes);
            int bloomLen = bloomBytes.length;
            channel.write(bloomBuf);

            // 5. Write Footer
            long createdAt = System.currentTimeMillis();
            SSTableMetadata metadata = new SSTableMetadata(tableId, level, minKey, maxKey,
                    minSeq == Long.MAX_VALUE ? 0 : minSeq,
                    maxSeq == Long.MIN_VALUE ? 0 : maxSeq,
                    recordCount, dataSize, indexOffset, indexLen, bloomOffset, bloomLen, createdAt);

            ByteBuffer footerBuf = metadata.serialize();
            int footerLen = footerBuf.remaining();
            channel.write(footerBuf);

            // 6. Write trailing 4-byte Footer Length
            ByteBuffer footerLenBuf = ByteBuffer.allocate(4);
            footerLenBuf.putInt(footerLen);
            footerLenBuf.flip();
            channel.write(footerLenBuf);

            channel.force(true);
            return metadata;
        }
    }

    private void flushDataBlock(FileChannel channel, DataBlock block, IndexBlock indexBlock) throws IOException {
        long offset = channel.position();
        ByteBuffer blockBuf = block.serialize();
        int length = blockBuf.remaining();
        channel.write(blockBuf);
        indexBlock.addEntry(block.getLastKey(), offset, length);
    }
}
