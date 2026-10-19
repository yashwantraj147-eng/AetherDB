package com.aetherdb.storage.wal;

import com.aetherdb.common.AetherConfig;
import com.aetherdb.common.Constants;
import com.aetherdb.model.DataRecord;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * High-performance append-only Write-Ahead Log (WAL) with CRC32 integrity checks,
 * configurable sync modes (IMMEDIATE, PERIODIC, OS_BUFFER), and crash recovery.
 */
public class WriteAheadLog implements AutoCloseable {

    private final File walFile;
    private final AetherConfig.WalSyncMode syncMode;
    private final FileChannel channel;
    private final ReentrantLock appendLock = new ReentrantLock();
    private final ScheduledExecutorService periodicSyncExecutor;

    private volatile boolean closed = false;

    public WriteAheadLog(File walFile, AetherConfig.WalSyncMode syncMode, long periodicSyncMs) throws IOException {
        this.walFile = walFile;
        this.syncMode = syncMode;

        boolean isNew = !walFile.exists() || walFile.length() == 0;
        this.channel = FileChannel.open(walFile.toPath(),
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE);

        if (isNew) {
            writeHeader();
        } else {
            channel.position(channel.size());
        }

        if (syncMode == AetherConfig.WalSyncMode.PERIODIC) {
            this.periodicSyncExecutor = Executors.newSingleThreadScheduledExecutor(
                    Thread.ofVirtual().name("wal-sync-", 1).factory()
            );
            this.periodicSyncExecutor.scheduleWithFixedDelay(this::syncSafe, periodicSyncMs, periodicSyncMs, TimeUnit.MILLISECONDS);
        } else {
            this.periodicSyncExecutor = null;
        }
    }

    private void writeHeader() throws IOException {
        ByteBuffer header = ByteBuffer.allocate(6);
        header.putInt(Constants.WAL_MAGIC);
        header.putShort(Constants.WAL_VERSION);
        header.flip();
        channel.write(header);
        channel.force(true);
    }

    /**
     * Appends a record to the WAL.
     */
    public void append(DataRecord record) throws IOException {
        if (closed) {
            throw new IllegalStateException("WAL is closed");
        }

        LogEntry entry = new LogEntry(record);
        int frameSize = 8 + record.estimatedSize() + 32;
        ByteBuffer buffer = ByteBuffer.allocate(frameSize);
        entry.serialize(buffer);
        buffer.flip();

        appendLock.lock();
        try {
            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }
            if (syncMode == AetherConfig.WalSyncMode.IMMEDIATE) {
                channel.force(false);
            }
        } finally {
            appendLock.unlock();
        }
    }

    /**
     * Flushes buffered bytes to persistent disk storage.
     */
    public void sync() throws IOException {
        appendLock.lock();
        try {
            if (channel.isOpen()) {
                channel.force(false);
            }
        } finally {
            appendLock.unlock();
        }
    }

    private void syncSafe() {
        try {
            sync();
        } catch (IOException ignored) {}
    }

    /**
     * Reads all valid records during crash recovery.
     */
    public List<DataRecord> recover() throws IOException {
        List<DataRecord> records = new ArrayList<>();
        appendLock.lock();
        try {
            if (channel.size() < 6) {
                return records;
            }

            channel.position(0);
            ByteBuffer header = ByteBuffer.allocate(6);
            channel.read(header);
            header.flip();

            int magic = header.getInt();
            short version = header.getShort();
            if (magic != Constants.WAL_MAGIC) {
                throw new IllegalStateException("Invalid WAL magic bytes: 0x" + Integer.toHexString(magic));
            }

            // Read the rest into buffer
            long dataRemaining = channel.size() - 6;
            if (dataRemaining <= 0) return records;

            ByteBuffer dataBuffer = ByteBuffer.allocate((int) dataRemaining);
            channel.read(dataBuffer);
            dataBuffer.flip();

            while (dataBuffer.hasRemaining()) {
                LogEntry entry = LogEntry.deserialize(dataBuffer);
                if (entry == null) {
                    break; // Corrupted or truncated tail entry
                }
                records.add(entry.getRecord());
            }

            // Move channel position back to end for further appends
            channel.position(channel.size());
            return records;
        } finally {
            appendLock.unlock();
        }
    }

    public File getWalFile() {
        return walFile;
    }

    public long size() {
        try {
            return channel.size();
        } catch (IOException e) {
            return 0;
        }
    }

    @Override
    public void close() throws IOException {
        if (closed) return;
        closed = true;

        if (periodicSyncExecutor != null) {
            periodicSyncExecutor.shutdown();
            try {
                if (!periodicSyncExecutor.awaitTermination(500, TimeUnit.MILLISECONDS)) {
                    periodicSyncExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        appendLock.lock();
        try {
            if (channel.isOpen()) {
                channel.force(true);
                channel.close();
            }
        } finally {
            appendLock.unlock();
        }
    }
}
