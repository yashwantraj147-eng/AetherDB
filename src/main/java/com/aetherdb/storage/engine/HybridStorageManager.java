package com.aetherdb.storage.engine;

import com.aetherdb.common.AetherConfig;
import com.aetherdb.common.BinaryUtils;
import com.aetherdb.common.Constants;
import com.aetherdb.model.*;
import com.aetherdb.storage.columnar.ColumnEncoder;
import com.aetherdb.storage.columnar.ColumnarChunk;
import com.aetherdb.storage.columnar.ZoneMap;
import com.aetherdb.storage.memtable.MemTable;
import com.aetherdb.storage.memtable.MemTableFlushTask;
import com.aetherdb.storage.sstable.CompactionEngine;
import com.aetherdb.storage.sstable.SSTableReader;
import com.aetherdb.storage.wal.WriteAheadLog;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Pattern;

/**
 * Production-grade Hybrid Storage Manager integrating:
 * 1. LSM-Tree Key-Value Engine with multi-segment WAL, MemTable, multi-level SSTables, and Bloom filters.
 * 2. Columnar Analytics Engine with RLE, Dictionary encoding, and Zone Maps.
 * 3. Asynchronous background compaction and virtual-thread execution with bounded backpressure.
 *
 * Durability and Concurrency Guarantees:
 * - WAL rotation and multi-segment replay across restarts.
 * - Sequence numbers persist monotonically across restarts and compactions.
 * - Atomic write ordering: sequence allocation, WAL append, and MemTable insertion are atomic.
 * - Version-aware point lookups: selects newest record across all storage layers; tombstones hide older values.
 * - Compaction isolation: concurrent flushes cannot be clobbered by compaction publications.
 */
public class HybridStorageManager implements StorageEngine {

    private static final Pattern TABLE_NAME_PATTERN = Pattern.compile("^[a-zA-Z0-9_]{1,64}$");

    private final AetherConfig config;
    private final File dataDir;
    private final File walDir;
    private final File sstableDir;
    private final File columnarDir;

    private final AtomicLong sequenceNumberGenerator = new AtomicLong(1);
    private final AtomicLong sstableIdGenerator = new AtomicLong(1);
    private final AtomicLong chunkIdGenerator = new AtomicLong(1);
    private final AtomicLong walSegmentIdGenerator = new AtomicLong(1);

    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final ThreadPoolExecutor backgroundExecutor;

    // LSM Components
    private WriteAheadLog wal;
    private MemTable activeMemTable;
    private final List<MemTable> immutableMemTables = new CopyOnWriteArrayList<>();
    private final List<SSTableReader> sstables = new CopyOnWriteArrayList<>();

    // Columnar Store Components: TableName -> List of Chunks
    private final ConcurrentHashMap<String, List<ColumnarChunk>> columnarTables = new ConcurrentHashMap<>();

    // Metrics counters
    private final AtomicLong totalPuts = new AtomicLong(0);
    private final AtomicLong totalGets = new AtomicLong(0);
    private final AtomicLong totalDeletes = new AtomicLong(0);
    private final AtomicLong memtableHits = new AtomicLong(0);
    private final AtomicLong sstableHits = new AtomicLong(0);
    private final AtomicLong bloomFilterMisses = new AtomicLong(0);

    public HybridStorageManager(AetherConfig config) throws IOException {
        this.config = config;
        this.dataDir = new File(config.getStorageDataDir());
        this.walDir = new File(config.getStorageWalDir());
        this.sstableDir = new File(config.getStorageSSTableDir());
        this.columnarDir = new File(config.getStorageColumnarDir());

        config.validateAndCreateDirectories();

        // Bounded thread pool executor with CallerRuns backpressure
        int queueCapacity = Math.max(4, config.getFlushQueueCapacity());
        this.backgroundExecutor = new ThreadPoolExecutor(
                2, 4,
                60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(queueCapacity),
                Thread.ofVirtual().name("aether-storage-worker-", 1).factory(),
                new ThreadPoolExecutor.CallerRunsPolicy()
        );

        this.activeMemTable = new MemTable(System.currentTimeMillis());

        // Clean up any stale temporary files from prior interrupted operations
        cleanTemporaryFiles();

        // Load existing SSTables and find maximum sequence number in SSTables
        long maxSstableSeq = loadExistingSSTables();

        // Recover all valid WAL segments in chronological sequence
        long maxWalSeq = recoverFromAllWalSegments();

        // Ensure sequence numbers strictly exceed every persisted and recovered record
        long initialSeq = Math.max(maxSstableSeq, maxWalSeq) + 1;
        sequenceNumberGenerator.set(initialSeq);

        // Initialize active WAL segment
        initActiveWal();

        // Load existing Columnar chunks
        loadExistingColumnarChunks();
    }

    private void cleanTemporaryFiles() {
        cleanDirTmp(sstableDir);
        cleanDirTmp(columnarDir);
    }

    private void cleanDirTmp(File dir) {
        File[] tmpFiles = dir.listFiles((d, name) -> name.endsWith(".tmp"));
        if (tmpFiles != null) {
            for (File f : tmpFiles) {
                f.delete();
            }
        }
    }

    private void initActiveWal() throws IOException {
        long nextSegId = walSegmentIdGenerator.getAndIncrement();
        File walFile = new File(walDir, WriteAheadLog.formatSegmentFilename(nextSegId));
        this.wal = new WriteAheadLog(walFile, config.getWalSyncMode(), config.getWalPeriodicSyncMs());
    }

    private long recoverFromAllWalSegments() throws IOException {
        List<DataRecord> records = WriteAheadLog.recoverAllSegments(walDir);
        long maxSeq = 0;
        for (DataRecord r : records) {
            activeMemTable.put(r);
            if (r.getSequenceNumber() > maxSeq) {
                maxSeq = r.getSequenceNumber();
            }
        }

        // Determine next WAL segment ID based on existing files
        File[] walFiles = walDir.listFiles((dir, name) -> name.endsWith(Constants.WAL_FILE_EXT));
        long maxSegId = 0;
        if (walFiles != null) {
            for (File f : walFiles) {
                long segId = WriteAheadLog.parseSegmentId(f.getName());
                if (segId > maxSegId) maxSegId = segId;
            }
        }
        walSegmentIdGenerator.set(maxSegId + 1);

        return maxSeq;
    }

    private long loadExistingSSTables() {
        File[] files = sstableDir.listFiles((dir, name) ->
                name.endsWith(Constants.SSTABLE_DATA_EXT) && !name.endsWith(".tmp"));
        if (files == null) return 0;

        Arrays.sort(files, Comparator.comparing(File::getName).reversed());
        long maxId = 0;
        long maxSeq = 0;

        for (File f : files) {
            try {
                SSTableReader reader = new SSTableReader(f);
                sstables.add(reader);
                if (reader.getTableId() > maxId) {
                    maxId = reader.getTableId();
                }
                if (reader.getMetadata().getMaxSequenceNumber() > maxSeq) {
                    maxSeq = reader.getMetadata().getMaxSequenceNumber();
                }
            } catch (IOException e) {
                System.err.println("Failed to load existing SSTable: " + f.getName() + " - " + e.getMessage());
            }
        }
        sstableIdGenerator.set(maxId + 1);
        return maxSeq;
    }

    private void loadExistingColumnarChunks() {
        File[] files = columnarDir.listFiles((dir, name) ->
                name.endsWith(Constants.COLUMNAR_CHUNK_EXT) && !name.endsWith(".tmp"));
        if (files == null) return;

        long maxChunkId = 0;
        for (File f : files) {
            try {
                ColumnarChunk chunk = ColumnarChunk.readFromFile(f);
                String tableName = chunk.getSchema().getTableName();
                columnarTables.computeIfAbsent(tableName, k -> new CopyOnWriteArrayList<>()).add(chunk);
                if (chunk.getChunkId() > maxChunkId) {
                    maxChunkId = chunk.getChunkId();
                }
            } catch (IOException e) {
                System.err.println("Failed to load columnar chunk: " + f.getName() + " - " + e.getMessage());
            }
        }
        chunkIdGenerator.set(maxChunkId + 1);
    }

    @Override
    public void put(byte[] key, byte[] value) throws IOException {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }

        rwLock.writeLock().lock();
        try {
            long seq = sequenceNumberGenerator.getAndIncrement();
            DataRecord record = DataRecord.put(key, value, seq);

            wal.append(record);
            activeMemTable.put(record);
            totalPuts.incrementAndGet();

            if (activeMemTable.getByteSize() >= config.getMemTableMaxBytes()) {
                triggerMemTableFlush();
            }
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @Override
    public void put(String key, String value) throws IOException {
        put(key.getBytes(Constants.DEFAULT_CHARSET),
            value != null ? value.getBytes(Constants.DEFAULT_CHARSET) : null);
    }

    @Override
    public DataRecord get(byte[] key) throws IOException {
        if (key == null) return null;
        totalGets.incrementAndGet();

        rwLock.readLock().lock();
        try {
            DataRecord newestCandidate = null;

            // 1. Search active MemTable
            DataRecord memRecord = activeMemTable.get(key);
            if (memRecord != null) {
                newestCandidate = memRecord;
            }

            // 2. Search immutable MemTables currently flushing
            for (MemTable imm : immutableMemTables) {
                DataRecord immRecord = imm.get(key);
                if (immRecord != null) {
                    if (newestCandidate == null || immRecord.getSequenceNumber() > newestCandidate.getSequenceNumber()) {
                        newestCandidate = immRecord;
                    }
                }
            }

            // 3. Search SSTables (from newest to oldest)
            for (SSTableReader sstable : sstables) {
                if (newestCandidate != null && sstable.getMetadata().getMaxSequenceNumber() <= newestCandidate.getSequenceNumber()) {
                    continue;
                }
                DataRecord sstRecord = sstable.get(key);
                if (sstRecord != null) {
                    sstableHits.incrementAndGet();
                    if (newestCandidate == null || sstRecord.getSequenceNumber() > newestCandidate.getSequenceNumber()) {
                        newestCandidate = sstRecord;
                    }
                } else {
                    bloomFilterMisses.incrementAndGet();
                }
            }

            if (newestCandidate != null) {
                memtableHits.incrementAndGet();
                return newestCandidate.isTombstone() ? null : newestCandidate;
            }

            return null;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    @Override
    public DataRecord get(String key) throws IOException {
        return get(key.getBytes(Constants.DEFAULT_CHARSET));
    }

    @Override
    public void delete(byte[] key) throws IOException {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }

        rwLock.writeLock().lock();
        try {
            long seq = sequenceNumberGenerator.getAndIncrement();
            DataRecord record = DataRecord.delete(key, seq);

            wal.append(record);
            activeMemTable.put(record);
            totalDeletes.incrementAndGet();

            if (activeMemTable.getByteSize() >= config.getMemTableMaxBytes()) {
                triggerMemTableFlush();
            }
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @Override
    public void delete(String key) throws IOException {
        delete(key.getBytes(Constants.DEFAULT_CHARSET));
    }

    /**
     * Seals the active MemTable, rotates the WAL segment, queues background flush,
     * and associates the sealed WAL file for safe retirement only after durable flush.
     * Must be called under rwLock.writeLock().
     */
    private void triggerMemTableFlush() {
        if (activeMemTable.isEmpty()) return;

        MemTable sealedMemTable = activeMemTable;
        activeMemTable = new MemTable(System.currentTimeMillis());
        immutableMemTables.add(sealedMemTable);

        File rotatedWal = null;
        try {
            long nextSegId = walSegmentIdGenerator.getAndIncrement();
            rotatedWal = wal.rotate(nextSegId);
        } catch (IOException e) {
            System.err.println("WAL rotation error: " + e.getMessage());
        }

        final File walSegmentToRetire = rotatedWal;
        backgroundExecutor.submit(() -> {
            try {
                long tableId = sstableIdGenerator.getAndIncrement();
                MemTableFlushTask flushTask = new MemTableFlushTask(
                        sealedMemTable, sstableDir, tableId, config, walSegmentToRetire);
                SSTableReader newReader = flushTask.execute();

                rwLock.writeLock().lock();
                try {
                    sstables.add(0, newReader); // Newest first
                    immutableMemTables.remove(sealedMemTable);
                } finally {
                    rwLock.writeLock().unlock();
                }

                // Check if compaction is needed
                if (sstables.size() >= config.getCompactionMinThreshold()) {
                    compact();
                }
            } catch (Exception e) {
                System.err.println("Background MemTable flush failed: " + e.getMessage());
            }
        });
    }

    @Override
    public void flushMemTable() throws IOException {
        rwLock.writeLock().lock();
        try {
            // Flush any pending immutable MemTables first
            for (MemTable imm : new ArrayList<>(immutableMemTables)) {
                long tableId = sstableIdGenerator.getAndIncrement();
                MemTableFlushTask flushTask = new MemTableFlushTask(imm, sstableDir, tableId, config, null);
                SSTableReader newReader = flushTask.execute();
                sstables.add(0, newReader);
                immutableMemTables.remove(imm);
            }

            // Flush active MemTable if not empty
            if (!activeMemTable.isEmpty()) {
                MemTable sealedMemTable = activeMemTable;
                activeMemTable = new MemTable(System.currentTimeMillis());

                File rotatedWal = null;
                try {
                    long nextSegId = walSegmentIdGenerator.getAndIncrement();
                    rotatedWal = wal.rotate(nextSegId);
                } catch (IOException e) {
                    System.err.println("WAL rotation error in flushMemTable: " + e.getMessage());
                }

                long tableId = sstableIdGenerator.getAndIncrement();
                MemTableFlushTask flushTask = new MemTableFlushTask(
                        sealedMemTable, sstableDir, tableId, config, rotatedWal);
                SSTableReader newReader = flushTask.execute();
                sstables.add(0, newReader);
            }
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @Override
    public synchronized void compact() throws IOException {
        List<SSTableReader> tablesToCompact;
        rwLock.readLock().lock();
        try {
            if (sstables.size() < 2) return;
            tablesToCompact = new ArrayList<>(sstables);
        } finally {
            rwLock.readLock().unlock();
        }

        long newTableId = sstableIdGenerator.getAndIncrement();
        String targetFilename = String.format("sstable_%06d%s", newTableId, Constants.SSTABLE_DATA_EXT);
        File targetFile = new File(sstableDir, targetFilename);

        // Prune tombstones only if all existing tables are compacted together
        boolean isDeepestLevel = (tablesToCompact.size() == sstables.size());

        SSTableReader compacted = CompactionEngine.compact(
                tablesToCompact,
                targetFile,
                newTableId,
                1, // Level 1
                config.getSSTableBlockSize(),
                config.getBloomFpr(),
                isDeepestLevel
        );

        // Atomically publish compacted table without clobbering concurrent flushes
        rwLock.writeLock().lock();
        try {
            sstables.removeAll(tablesToCompact);
            sstables.add(compacted);
            // Sort SSTables newest first by table ID
            sstables.sort((a, b) -> Long.compare(b.getTableId(), a.getTableId()));
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @Override
    public void ingestColumnar(String tableName, List<Map<String, ColumnValue>> records) throws IOException {
        if (tableName == null || !TABLE_NAME_PATTERN.matcher(tableName).matches()) {
            throw new IllegalArgumentException("Invalid table name: '" + tableName + "'. Must be alphanumeric and up to 64 chars.");
        }
        if (records == null || records.isEmpty()) return;

        // Partition ingestion into batches of at most config.getColumnarMaxRows()
        int maxRowsPerChunk = Math.max(1, config.getColumnarMaxRows());
        for (int i = 0; i < records.size(); i += maxRowsPerChunk) {
            int toIndex = Math.min(i + maxRowsPerChunk, records.size());
            List<Map<String, ColumnValue>> subBatch = records.subList(i, toIndex);
            ingestColumnarChunk(tableName, subBatch);
        }
    }

    private void ingestColumnarChunk(String tableName, List<Map<String, ColumnValue>> records) throws IOException {
        // Validate and infer schema from first row
        Map<String, ColumnValue> first = records.get(0);
        List<ColumnDefinition> colDefs = new ArrayList<>();
        Map<String, ColumnType> expectedTypes = new HashMap<>();

        for (Map.Entry<String, ColumnValue> entry : first.entrySet()) {
            colDefs.add(new ColumnDefinition(entry.getKey(), entry.getValue().getType()));
            expectedTypes.put(entry.getKey(), entry.getValue().getType());
        }
        Schema schema = new Schema(tableName, colDefs);

        // Validate all rows against expected schema and types
        Map<String, List<ColumnValue>> colData = new HashMap<>();
        Map<String, ZoneMap> zoneMaps = new HashMap<>();
        for (ColumnDefinition col : colDefs) {
            colData.put(col.getName(), new ArrayList<>(records.size()));
            zoneMaps.put(col.getName(), new ZoneMap(col.getType()));
        }

        for (int rowIdx = 0; rowIdx < records.size(); rowIdx++) {
            Map<String, ColumnValue> row = records.get(rowIdx);
            for (ColumnDefinition col : colDefs) {
                ColumnValue val = row.get(col.getName());
                if (val == null) {
                    val = ColumnValue.ofNull(col.getType());
                } else if (!val.isNull() && val.getType() != col.getType()) {
                    throw new IllegalArgumentException("Schema type mismatch at row " + rowIdx + ", column '"
                            + col.getName() + "': expected " + col.getType() + " but got " + val.getType());
                }
                colData.get(col.getName()).add(val);
                zoneMaps.get(col.getName()).update(val);
            }
        }

        // Encode columns
        Map<String, byte[]> encodedColumns = new HashMap<>();
        for (ColumnDefinition col : colDefs) {
            ColumnEncoder encoder = new ColumnEncoder(col.getType(), config.getDictionaryThresholdRatio());
            byte[] encoded = encoder.encode(colData.get(col.getName()));
            encodedColumns.put(col.getName(), encoded);
        }

        long chunkId = chunkIdGenerator.getAndIncrement();
        ColumnarChunk chunk = new ColumnarChunk(chunkId, schema, records.size(), zoneMaps, encodedColumns);

        String chunkFilename = String.format("%s_%06d%s", tableName, chunkId, Constants.COLUMNAR_CHUNK_EXT);
        File chunkFile = new File(columnarDir, chunkFilename);
        chunk.writeToFile(chunkFile);

        columnarTables.computeIfAbsent(tableName, k -> new CopyOnWriteArrayList<>()).add(chunk);
    }

    @Override
    public List<ColumnarChunk> getColumnarChunks(String tableName) {
        if (tableName == null) return Collections.emptyList();
        List<ColumnarChunk> chunks = columnarTables.get(tableName);
        return chunks != null ? chunks : Collections.emptyList();
    }

    @Override
    public Map<String, Object> getMetrics() {
        Map<String, Object> metrics = new HashMap<>();
        metrics.put("totalPuts", totalPuts.get());
        metrics.put("totalGets", totalGets.get());
        metrics.put("totalDeletes", totalDeletes.get());
        metrics.put("memtableHits", memtableHits.get());
        metrics.put("sstableHits", sstableHits.get());
        metrics.put("bloomFilterMisses", bloomFilterMisses.get());
        metrics.put("memtableBytes", activeMemTable.getByteSize());
        metrics.put("memtableRecords", activeMemTable.size());
        metrics.put("sstableCount", sstables.size());
        metrics.put("columnarTablesCount", columnarTables.size());

        long totalColumnarRows = 0;
        for (List<ColumnarChunk> chunks : columnarTables.values()) {
            for (ColumnarChunk c : chunks) {
                totalColumnarRows += c.getRowCount();
            }
        }
        metrics.put("totalColumnarRows", totalColumnarRows);

        return metrics;
    }

    @Override
    public void close() throws IOException {
        backgroundExecutor.shutdown();
        try {
            if (!backgroundExecutor.awaitTermination(3, TimeUnit.SECONDS)) {
                backgroundExecutor.shutdownNow();
                if (!backgroundExecutor.awaitTermination(2, TimeUnit.SECONDS)) {
                    System.err.println("Background executor did not terminate cleanly");
                }
            }
        } catch (InterruptedException e) {
            backgroundExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        rwLock.writeLock().lock();
        try {
            if (wal != null) {
                wal.close();
            }
            for (SSTableReader reader : sstables) {
                reader.close();
            }
        } finally {
            rwLock.writeLock().unlock();
        }
    }
}
