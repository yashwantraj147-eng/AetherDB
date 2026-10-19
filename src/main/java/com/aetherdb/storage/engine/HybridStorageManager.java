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

/**
 * Production-grade Hybrid Storage Manager integrating:
 * 1. LSM-Tree Key-Value Engine with WAL, MemTable, multi-level SSTables, and Bloom filters.
 * 2. Columnar Analytics Engine with RLE, Dictionary encoding, and Zone Maps.
 * 3. Asynchronous background compaction and virtual-thread execution.
 */
public class HybridStorageManager implements StorageEngine {

    private final AetherConfig config;
    private final File dataDir;
    private final File walDir;
    private final File sstableDir;
    private final File columnarDir;

    private final AtomicLong sequenceNumberGenerator = new AtomicLong(1);
    private final AtomicLong sstableIdGenerator = new AtomicLong(1);
    private final AtomicLong chunkIdGenerator = new AtomicLong(1);

    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final ExecutorService backgroundExecutor;

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

        this.backgroundExecutor = Executors.newFixedThreadPool(4,
                Thread.ofVirtual().name("aether-storage-worker-", 1).factory()
        );

        this.activeMemTable = new MemTable(System.currentTimeMillis());

        // Initialize WAL
        File walFile = new File(walDir, "active" + Constants.WAL_FILE_EXT);
        this.wal = new WriteAheadLog(walFile, config.getWalSyncMode(), config.getWalPeriodicSyncMs());

        // Recover from WAL if it has prior records
        recoverFromWal();

        // Load existing SSTables
        loadExistingSSTables();

        // Load existing Columnar chunks
        loadExistingColumnarChunks();
    }

    private void recoverFromWal() throws IOException {
        List<DataRecord> records = wal.recover();
        long maxSeq = 0;
        for (DataRecord r : records) {
            activeMemTable.put(r);
            if (r.getSequenceNumber() > maxSeq) {
                maxSeq = r.getSequenceNumber();
            }
        }
        sequenceNumberGenerator.set(maxSeq + 1);
    }

    private void loadExistingSSTables() {
        File[] files = sstableDir.listFiles((dir, name) -> name.endsWith(Constants.SSTABLE_DATA_EXT));
        if (files == null) return;

        Arrays.sort(files, Comparator.comparing(File::getName).reversed());
        long maxId = 0;

        for (File f : files) {
            try {
                SSTableReader reader = new SSTableReader(f);
                sstables.add(reader);
                if (reader.getTableId() > maxId) {
                    maxId = reader.getTableId();
                }
            } catch (IOException e) {
                System.err.println("Failed to load existing SSTable: " + f.getName() + " - " + e.getMessage());
            }
        }
        sstableIdGenerator.set(maxId + 1);
    }

    private void loadExistingColumnarChunks() {
        File[] files = columnarDir.listFiles((dir, name) -> name.endsWith(Constants.COLUMNAR_CHUNK_EXT));
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
        long seq = sequenceNumberGenerator.getAndIncrement();
        DataRecord record = DataRecord.put(key, value, seq);

        rwLock.writeLock().lock();
        try {
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
        put(key.getBytes(Constants.DEFAULT_CHARSET), value.getBytes(Constants.DEFAULT_CHARSET));
    }

    @Override
    public DataRecord get(byte[] key) throws IOException {
        totalGets.incrementAndGet();

        rwLock.readLock().lock();
        try {
            // 1. Search active MemTable
            DataRecord memRecord = activeMemTable.get(key);
            if (memRecord != null) {
                memtableHits.incrementAndGet();
                return memRecord.isTombstone() ? null : memRecord;
            }

            // 2. Search immutable MemTables currently flushing
            for (MemTable imm : immutableMemTables) {
                DataRecord immRecord = imm.get(key);
                if (immRecord != null) {
                    memtableHits.incrementAndGet();
                    return immRecord.isTombstone() ? null : immRecord;
                }
            }

            // 3. Search SSTables (from newest to oldest)
            for (SSTableReader sstable : sstables) {
                DataRecord sstRecord = sstable.get(key);
                if (sstRecord != null) {
                    sstableHits.incrementAndGet();
                    return sstRecord.isTombstone() ? null : sstRecord;
                } else {
                    bloomFilterMisses.incrementAndGet();
                }
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
        long seq = sequenceNumberGenerator.getAndIncrement();
        DataRecord record = DataRecord.delete(key, seq);

        rwLock.writeLock().lock();
        try {
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

    private void triggerMemTableFlush() {
        MemTable sealedMemTable = activeMemTable;
        activeMemTable = new MemTable(System.currentTimeMillis());
        immutableMemTables.add(sealedMemTable);

        // Rotate WAL
        try {
            wal.close();
            File oldWal = wal.getWalFile();
            File rotatedWal = new File(walDir, "wal_" + System.currentTimeMillis() + Constants.WAL_FILE_EXT);
            oldWal.renameTo(rotatedWal);

            File newWal = new File(walDir, "active" + Constants.WAL_FILE_EXT);
            wal = new WriteAheadLog(newWal, config.getWalSyncMode(), config.getWalPeriodicSyncMs());
        } catch (IOException e) {
            System.err.println("WAL rotation warning: " + e.getMessage());
        }

        backgroundExecutor.submit(() -> {
            try {
                long tableId = sstableIdGenerator.getAndIncrement();
                MemTableFlushTask flushTask = new MemTableFlushTask(sealedMemTable, sstableDir, tableId, config);
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
            // Flush and drain any pending immutable MemTables
            for (MemTable imm : new ArrayList<>(immutableMemTables)) {
                long tableId = sstableIdGenerator.getAndIncrement();
                MemTableFlushTask flushTask = new MemTableFlushTask(imm, sstableDir, tableId, config);
                SSTableReader newReader = flushTask.execute();
                sstables.add(0, newReader);
                immutableMemTables.remove(imm);
            }

            // Flush active MemTable if not empty
            if (!activeMemTable.isEmpty()) {
                MemTable sealedMemTable = activeMemTable;
                activeMemTable = new MemTable(System.currentTimeMillis());

                long tableId = sstableIdGenerator.getAndIncrement();
                MemTableFlushTask flushTask = new MemTableFlushTask(sealedMemTable, sstableDir, tableId, config);
                SSTableReader newReader = flushTask.execute();
                sstables.add(0, newReader);
            }
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @Override
    public synchronized void compact() throws IOException {
        if (sstables.size() < 2) return;

        List<SSTableReader> tablesToCompact = new ArrayList<>(sstables);
        long newTableId = sstableIdGenerator.getAndIncrement();
        String targetFilename = String.format("sstable_%06d%s", newTableId, Constants.SSTABLE_DATA_EXT);
        File targetFile = new File(sstableDir, targetFilename);

        SSTableReader compacted = CompactionEngine.compact(
                tablesToCompact,
                targetFile,
                newTableId,
                1, // Level 1
                config.getSSTableBlockSize(),
                config.getBloomFpr(),
                true // Deepest level, prune tombstones
        );

        rwLock.writeLock().lock();
        try {
            sstables.clear();
            sstables.add(compacted);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @Override
    public void ingestColumnar(String tableName, List<Map<String, ColumnValue>> records) throws IOException {
        if (records.isEmpty()) return;

        // Infer schema
        Map<String, ColumnValue> first = records.get(0);
        List<ColumnDefinition> colDefs = new ArrayList<>();
        for (Map.Entry<String, ColumnValue> entry : first.entrySet()) {
            colDefs.add(new ColumnDefinition(entry.getKey(), entry.getValue().getType()));
        }
        Schema schema = new Schema(tableName, colDefs);

        // Group values per column
        Map<String, List<ColumnValue>> colData = new HashMap<>();
        Map<String, ZoneMap> zoneMaps = new HashMap<>();
        for (ColumnDefinition col : colDefs) {
            colData.put(col.getName(), new ArrayList<>(records.size()));
            zoneMaps.put(col.getName(), new ZoneMap(col.getType()));
        }

        for (Map<String, ColumnValue> row : records) {
            for (ColumnDefinition col : colDefs) {
                ColumnValue val = row.get(col.getName());
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
            if (!backgroundExecutor.awaitTermination(2, TimeUnit.SECONDS)) {
                backgroundExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        rwLock.writeLock().lock();
        try {
            if (wal != null) wal.close();
            for (SSTableReader reader : sstables) {
                reader.close();
            }
        } finally {
            rwLock.writeLock().unlock();
        }
    }
}
