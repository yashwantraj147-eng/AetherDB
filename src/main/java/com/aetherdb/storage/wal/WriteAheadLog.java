package com.aetherdb.storage.wal;

import com.aetherdb.common.AetherConfig;
import com.aetherdb.common.Constants;
import com.aetherdb.model.DataRecord;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.CRC32;

/**
 * High-performance append-only Write-Ahead Log (WAL) with CRC32 integrity checks,
 * bounded recovery, safe torn-tail truncation, monotonic segment rotation,
 * and configurable durability sync modes (IMMEDIATE, PERIODIC, OS_BUFFER).
 *
 * Durability guarantee:
 * - IMMEDIATE: Guarantees synchronous fsync (channel.force) on every append before acknowledging write.
 * - PERIODIC: Flushes to OS buffer on append and fsyncs periodically on background timer.
 * - OS_BUFFER: Buffers in OS page cache; durable on graceful OS flush.
 *
 * Recovery semantics:
 * - Case A (Incomplete trailing frame): Demonstrably incomplete trailing header/payload at EOF is truncated and forced.
 * - Case B (Complete frame with incorrect CRC): Throws CorruptedWalException. Never silently truncated.
 * - Case C (Corruption inside WAL): Throws CorruptedWalException.
 */
public class WriteAheadLog implements AutoCloseable {

    private static final Pattern SEGMENT_PATTERN = Pattern.compile("wal_(\\d+)\\" + Constants.WAL_FILE_EXT);

    private final File walDir;
    private final AetherConfig.WalSyncMode syncMode;
    private final long periodicSyncMs;
    private final ReentrantLock appendLock = new ReentrantLock();

    private volatile File currentWalFile;
    private volatile FileChannel channel;
    private volatile long currentSegmentId;
    private volatile boolean closed = false;

    public boolean isClosed() {
        return closed;
    }

    private final ScheduledExecutorService periodicSyncExecutor;

    public WriteAheadLog(File walFile, AetherConfig.WalSyncMode syncMode, long periodicSyncMs) throws IOException {
        this.walDir = walFile.getParentFile();
        this.syncMode = syncMode;
        this.periodicSyncMs = periodicSyncMs;
        this.currentWalFile = walFile;
        this.currentSegmentId = parseSegmentId(walFile.getName());

        openCurrentFile();

        if (syncMode == AetherConfig.WalSyncMode.PERIODIC) {
            this.periodicSyncExecutor = Executors.newSingleThreadScheduledExecutor(
                    Thread.ofVirtual().name("wal-sync-", 1).factory()
            );
            this.periodicSyncExecutor.scheduleWithFixedDelay(this::syncSafe, periodicSyncMs, periodicSyncMs, TimeUnit.MILLISECONDS);
        } else {
            this.periodicSyncExecutor = null;
        }
    }

    private void openCurrentFile() throws IOException {
        boolean isNew = !currentWalFile.exists() || currentWalFile.length() == 0;
        this.channel = FileChannel.open(currentWalFile.toPath(),
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE);

        if (isNew) {
            writeHeader();
        } else {
            channel.position(channel.size());
        }
    }

    private void writeHeader() throws IOException {
        ByteBuffer header = ByteBuffer.allocate(6);
        header.putInt(Constants.WAL_MAGIC);
        header.putShort(Constants.WAL_VERSION);
        header.flip();
        writeFully(channel, header);
        channel.force(true);
    }

    private static void writeFully(FileChannel ch, ByteBuffer buf) throws IOException {
        while (buf.hasRemaining()) {
            ch.write(buf);
        }
    }

    private static int readFully(FileChannel ch, ByteBuffer buf) throws IOException {
        int totalRead = 0;
        while (buf.hasRemaining()) {
            int read = ch.read(buf);
            if (read == -1) {
                break;
            }
            totalRead += read;
        }
        return totalRead;
    }

    /**
     * Appends a record to the active WAL segment.
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
            if (closed) {
                throw new IllegalStateException("WAL is closed");
            }
            writeFully(channel, buffer);
            if (syncMode == AetherConfig.WalSyncMode.IMMEDIATE) {
                channel.force(false);
            }
        } finally {
            appendLock.unlock();
        }
    }

    /**
     * Flushes buffered bytes to persistent storage.
     */
    public void sync() throws IOException {
        appendLock.lock();
        try {
            if (channel != null && channel.isOpen()) {
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
     * Rotates the WAL to a new monotonically identified segment safely.
     * Pre-allocates and initializes the new segment BEFORE closing the current one.
     * If initialization fails, the active segment remains open and usable.
     * Returns the file of the rotated (now sealed) segment.
     */
    public File rotate(long nextSegmentId) throws IOException {
        appendLock.lock();
        try {
            if (closed) {
                throw new IllegalStateException("WAL is closed, cannot rotate");
            }

            // Pre-create and initialize the new segment BEFORE touching active segment
            String nextFilename = formatSegmentFilename(nextSegmentId);
            File nextWalFile = new File(walDir, nextFilename);
            FileChannel nextChannel = FileChannel.open(
                    nextWalFile.toPath(),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.READ,
                    StandardOpenOption.WRITE
            );

            try {
                ByteBuffer header = ByteBuffer.allocate(6);
                header.putInt(Constants.WAL_MAGIC);
                header.putShort(Constants.WAL_VERSION);
                header.flip();
                writeFully(nextChannel, header);
                nextChannel.force(true);
            } catch (IOException e) {
                try {
                    nextChannel.close();
                } catch (IOException ignored) {}
                nextWalFile.delete();
                throw new IOException("Failed to initialize new WAL segment: " + nextFilename, e);
            }

            // Flush and close previous active segment
            if (channel != null && channel.isOpen()) {
                channel.force(true);
                channel.close();
            }

            File rotatedFile = currentWalFile;
            this.channel = nextChannel;
            this.currentWalFile = nextWalFile;
            this.currentSegmentId = nextSegmentId;

            return rotatedFile;
        } finally {
            appendLock.unlock();
        }
    }

    /**
     * Recovers records from the current WAL file with bounded streaming memory usage.
     */
    public List<DataRecord> recover() throws IOException {
        List<DataRecord> records = new ArrayList<>();
        recoverFile(this.currentWalFile, this.channel, appendLock, records::add);
        return records;
    }

    /**
     * Recovers records from a specific WAL segment file into a list.
     */
    public static List<DataRecord> recoverFile(File file, FileChannel ch, ReentrantLock lock) throws IOException {
        List<DataRecord> records = new ArrayList<>();
        recoverFile(file, ch, lock, records::add);
        return records;
    }

    /**
     * Recovers records from a specific WAL segment file via a streaming consumer.
     * Distinguishes Case A (incomplete trailing frame), Case B (complete frame with bad CRC),
     * and Case C (corruption inside WAL).
     */
    public static void recoverFile(File file, FileChannel ch, ReentrantLock lock, Consumer<DataRecord> consumer) throws IOException {
        boolean managedChannel = (ch == null);
        FileChannel targetChannel = managedChannel
                ? FileChannel.open(file.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE)
                : ch;

        if (lock != null) lock.lock();
        try {
            long fileSize = targetChannel.size();
            if (fileSize < 6) {
                if (fileSize > 0) {
                    // Case A: Incomplete magic/version header at EOF
                    targetChannel.truncate(0);
                    targetChannel.force(true);
                }
                return;
            }

            targetChannel.position(0);
            ByteBuffer header = ByteBuffer.allocate(6);
            if (readFully(targetChannel, header) < 6) {
                targetChannel.truncate(0);
                targetChannel.force(true);
                return;
            }
            header.flip();

            int magic = header.getInt();
            short version = header.getShort();
            if (magic != Constants.WAL_MAGIC) {
                throw new CorruptedWalException("Invalid WAL magic bytes: 0x" + Integer.toHexString(magic) + " in " + file.getName());
            }
            if (version != Constants.WAL_VERSION) {
                throw new CorruptedWalException("Unsupported WAL version: " + version + " in " + file.getName());
            }

            long lastValidOffset = 6;
            ByteBuffer frameHeaderBuf = ByteBuffer.allocate(8);

            while (lastValidOffset < fileSize) {
                targetChannel.position(lastValidOffset);
                frameHeaderBuf.clear();
                int headerRead = readFully(targetChannel, frameHeaderBuf);
                if (headerRead == 0) {
                    // Clean EOF
                    break;
                }
                if (headerRead < 8) {
                    // Case A: Incomplete frame header at EOF (torn tail)
                    break;
                }
                frameHeaderBuf.flip();
                int expectedCrc = frameHeaderBuf.getInt();
                int payloadLen = frameHeaderBuf.getInt();

                if (payloadLen <= 0 || payloadLen > LogEntry.MAX_PAYLOAD_SIZE) {
                    // If header is at the very EOF
                    if (lastValidOffset + 8 >= fileSize) {
                        break;
                    }
                    throw new CorruptedWalException("Corrupt WAL frame payload length " + payloadLen + " at offset " + lastValidOffset + " in " + file.getName());
                }

                if (lastValidOffset + 8 + payloadLen > fileSize) {
                    // Case A: Incomplete payload at EOF (torn tail)
                    break;
                }

                ByteBuffer payloadBuf = ByteBuffer.allocate(payloadLen);
                int payloadRead = readFully(targetChannel, payloadBuf);
                if (payloadRead < payloadLen) {
                    // Case A: Incomplete payload at EOF
                    break;
                }
                payloadBuf.flip();

                CRC32 crc = new CRC32();
                crc.update(payloadBuf.array(), 0, payloadLen);
                long actualCrc = crc.getValue();

                if ((int) actualCrc != expectedCrc) {
                    // Case B: Complete frame present on disk but checksum mismatch!
                    // This is actual corruption, NOT a torn write. Fail recovery.
                    throw new CorruptedWalException("WAL CRC32 mismatch at offset " + lastValidOffset + " in " + file.getName()
                            + "! Expected=" + expectedCrc + ", Actual=" + (int) actualCrc);
                }

                DataRecord record = DataRecord.deserialize(payloadBuf);
                consumer.accept(record);
                lastValidOffset = targetChannel.position();
            }

            // Case A: Truncate demonstrably incomplete trailing bytes if any
            if (targetChannel.size() > lastValidOffset) {
                targetChannel.truncate(lastValidOffset);
                targetChannel.force(true);
            }
            targetChannel.position(lastValidOffset);
        } finally {
            if (managedChannel) {
                targetChannel.close();
            }
            if (lock != null) lock.unlock();
        }
    }

    /**
     * Recovers records across all valid WAL segments in the WAL directory in chronological sequence.
     */
    public static List<DataRecord> recoverAllSegments(File walDir) throws IOException {
        List<DataRecord> allRecords = new ArrayList<>();
        recoverAllSegments(walDir, allRecords::add);
        return allRecords;
    }

    /**
     * Recovers records across all valid WAL segments in the WAL directory using streaming callback.
     */
    public static void recoverAllSegments(File walDir, Consumer<DataRecord> consumer) throws IOException {
        File[] walFiles = walDir.listFiles((dir, name) -> name.endsWith(Constants.WAL_FILE_EXT));
        if (walFiles == null || walFiles.length == 0) {
            return;
        }

        // Sort files chronologically by segment sequence id
        Arrays.sort(walFiles, Comparator.comparingLong(f -> parseSegmentId(f.getName())));

        for (File f : walFiles) {
            recoverFile(f, null, null, consumer);
        }
    }

    public static String formatSegmentFilename(long segmentId) {
        return String.format("wal_%08d%s", segmentId, Constants.WAL_FILE_EXT);
    }

    public static long parseSegmentId(String filename) {
        if ("active.wal".equalsIgnoreCase(filename)) {
            return 1L;
        }
        Matcher matcher = SEGMENT_PATTERN.matcher(filename);
        if (matcher.matches()) {
            return Long.parseLong(matcher.group(1));
        }
        // Fallback for timestamps or arbitrary numbers
        String digits = filename.replaceAll("\\D+", "");
        if (!digits.isEmpty()) {
            try {
                return Long.parseLong(digits);
            } catch (NumberFormatException ignored) {}
        }
        return 1L;
    }

    public File getWalFile() {
        return currentWalFile;
    }

    public long getSegmentId() {
        return currentSegmentId;
    }

    public long size() {
        appendLock.lock();
        try {
            return channel != null && channel.isOpen() ? channel.size() : 0;
        } catch (IOException e) {
            return 0;
        } finally {
            appendLock.unlock();
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
            if (channel != null && channel.isOpen()) {
                channel.force(true);
                channel.close();
            }
        } finally {
            appendLock.unlock();
        }
    }
}
