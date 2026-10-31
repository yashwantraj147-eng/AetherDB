package com.aetherdb.storage.engine;

import com.aetherdb.common.AetherConfig;
import com.aetherdb.common.Constants;
import com.aetherdb.model.ColumnDefinition;
import com.aetherdb.model.ColumnType;
import com.aetherdb.model.ColumnValue;
import com.aetherdb.model.DataRecord;
import com.aetherdb.model.Schema;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Pattern;

/**
 * Hybrid Storage Engine coordinating:
 * 1. LSM-Tree Key-Value Engine (WAL -> Active MemTable -> Flushing MemTables -> SSTables -> Compaction)
 * 2. Columnar Analytics Engine (Chunk files with ZoneMap predicate pushdown)
 *
 * Implements strict lifecycle states (OPEN, CLOSING, CLOSED, FAILED), double-checked synchronization,
 * operation admission tracking, pre-rotated WAL swapping, safe bounded shutdown, and defensive serialization.
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
    private final AtomicInteger activeFlushTasks = new AtomicInteger(0);
    private final AtomicInteger activeOperations = new AtomicInteger(0);

    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final ThreadPoolExecutor backgroundExecutor;

    private final AtomicReference<LifecycleState> lifecycleState = new AtomicReference<>(LifecycleState.OPEN);
    private volatile String failureReason = null;

    // LSM Components
    private WriteAheadLog wal;
    private MemTable activeMemTable;
    private final List<MemTable> immutableMemTables = new CopyOnWriteArrayList<>();
    private final List<SSTableReader> sstables = new CopyOnWriteArrayList<>();

    // Columnar Store Components: TableName -> List of Chunks
    private final ConcurrentHashMap<String, List<ColumnarChunk>> columnarTables = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Schema> tableSchemas = new ConcurrentHashMap<>();

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

        // Recover all valid WAL segments in chronological sequence via streaming consumer
        long maxWalSeq = recoverFromAllWalSegments();

        // Ensure sequence numbers strictly exceed every persisted and recovered record
        long initialSeq = Math.max(maxSstableSeq, maxWalSeq) + 1;
        sequenceNumberGenerator.set(initialSeq);

        // Initialize active WAL segment
        initActiveWal();

        // Load existing Columnar chunks and reconstruct table schemas
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
        AtomicLong maxSeq = new AtomicLong(0);
        WriteAheadLog.recoverAllSegments(walDir, r -> {
            activeMemTable.put(r);
            if (r.getSequenceNumber() > maxSeq.get()) {
                maxSeq.set(r.getSequenceNumber());
            }
        });

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

        return maxSeq.get();
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
                tableSchemas.putIfAbsent(tableName, chunk.getSchema());
                if (chunk.getChunkId() > maxChunkId) {
                    maxChunkId = chunk.getChunkId();
                }
            } catch (IOException e) {
                System.err.println("Failed to load columnar chunk: " + f.getName() + " - " + e.getMessage());
            }
        }
        chunkIdGenerator.set(maxChunkId + 1);
    }

    private void enterOperation() {
        LifecycleState current = lifecycleState.get();
        if (current != LifecycleState.OPEN) {
            throw new IllegalStateException("Storage engine is " + current + (failureReason != null ? ": " + failureReason : ""));
        }
        activeOperations.incrementAndGet();
        if (lifecycleState.get() != LifecycleState.OPEN) {
            activeOperations.decrementAndGet();
            throw new IllegalStateException("Storage engine is " + lifecycleState.get() + (failureReason != null ? ": " + failureReason : ""));
        }
    }

    private void exitOperation() {
        activeOperations.decrementAndGet();
    }

    public int getActiveOperationCount() {
        return activeOperations.get();
    }

    @Override
    public LifecycleState getLifecycleState() {
        return lifecycleState.get();
    }

    @Override
    public void put(byte[] key, byte[] value) throws IOException {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }

        enterOperation();
        try {
            rwLock.writeLock().lock();
            try {
                LifecycleState state = lifecycleState.get();
                if (state == LifecycleState.CLOSING || state == LifecycleState.CLOSED) {
                    throw new IllegalStateException("Storage engine is closing or closed");
                }
                if (state == LifecycleState.FAILED) {
                    throw new IllegalStateException("Storage engine is in FAILED state: " + failureReason);
                }

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
        } finally {
            exitOperation();
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

        enterOperation();
        try {
            rwLock.readLock().lock();
            try {
                LifecycleState state = lifecycleState.get();
                if (state == LifecycleState.CLOSED) {
                    throw new IllegalStateException("Storage engine is closed");
                }
                if (state == LifecycleState.FAILED) {
                    throw new IllegalStateException("Storage engine is in FAILED state: " + failureReason);
                }

                totalGets.incrementAndGet();
                DataRecord newestCandidate = null;

                // 1. Search active MemTable
                DataRecord memRecord = activeMemTable.get(key);
                if (memRecord != null) {
                    newestCandidate = memRecord;
                }

                // 2. Search immutable MemTables currently in flight
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
        } finally {
            exitOperation();
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

        enterOperation();
        try {
            rwLock.writeLock().lock();
            try {
                LifecycleState state = lifecycleState.get();
                if (state == LifecycleState.CLOSING || state == LifecycleState.CLOSED) {
                    throw new IllegalStateException("Storage engine is closing or closed");
                }
                if (state == LifecycleState.FAILED) {
                    throw new IllegalStateException("Storage engine is in FAILED state: " + failureReason);
                }

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
        } finally {
            exitOperation();
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
    private void triggerMemTableFlush() throws IOException {
        if (activeMemTable.isEmpty()) return;

        // 1. Pre-rotate WAL BEFORE replacing activeMemTable to ensure crash safety
        long nextSegId = walSegmentIdGenerator.getAndIncrement();
        File rotatedWal;
        try {
            rotatedWal = wal.rotate(nextSegId);
        } catch (IOException e) {
            this.failureReason = "WAL rotation failed: " + e.getMessage();
            this.lifecycleState.set(LifecycleState.FAILED);
            System.err.println("CRITICAL: WAL rotation failed during flush trigger: " + e.getMessage());
            throw new IOException("WAL rotation failed; engine entered FAILED state to protect durability", e);
        }

        // 2. Safely seal activeMemTable and associate it with the rotated WAL segment
        MemTable sealedMemTable = activeMemTable;
        sealedMemTable.transitionTo(MemTable.State.ACTIVE, MemTable.State.SEALED);
        sealedMemTable.setAssociatedWalFile(rotatedWal);
        activeMemTable = new MemTable(System.currentTimeMillis());
        immutableMemTables.add(sealedMemTable);

        CompletableFuture<SSTableReader> flushFuture = sealedMemTable.claimFlushAttempt();
        if (flushFuture == null) {
            return;
        }

        final File walSegmentToRetire = rotatedWal;
        activeFlushTasks.incrementAndGet();

        backgroundExecutor.submit(() -> {
            try {
                long tableId = sstableIdGenerator.getAndIncrement();
                MemTableFlushTask flushTask = new MemTableFlushTask(
                        sealedMemTable, sstableDir, tableId, config, walSegmentToRetire);
                SSTableReader newReader = flushTask.execute();

                rwLock.writeLock().lock();
                try {
                    LifecycleState state = lifecycleState.get();
                    if (state != LifecycleState.OPEN) {
                        // Engine closed, closing, or failed before publication; close reader and preserve WAL!
                        newReader.close();
                        sealedMemTable.setState(MemTable.State.FAILED);
                        flushFuture.completeExceptionally(new IllegalStateException("Engine " + state + "; flush aborted before publication"));
                        return;
                    }

                    sstables.add(0, newReader); // Newest first
                    immutableMemTables.remove(sealedMemTable);
                    sealedMemTable.setState(MemTable.State.INSTALLED);
                    // Retire WAL segment strictly after successful publication in sstables
                    flushTask.retireAssociatedWal();
                    flushFuture.complete(newReader);
                } finally {
                    rwLock.writeLock().unlock();
                }

                // Check if compaction is needed
                if (sstables.size() >= config.getCompactionMinThreshold()) {
                    compact();
                }
            } catch (Throwable e) {
                sealedMemTable.setState(MemTable.State.FAILED);
                flushFuture.completeExceptionally(e);
                System.err.println("Background MemTable flush failed: " + e.getMessage());
            } finally {
                activeFlushTasks.decrementAndGet();
            }
        });
    }

    @Override
    public void flushMemTable() throws IOException {
        enterOperation();
        try {
            List<CompletableFuture<SSTableReader>> inFlightFutures = new ArrayList<>();
            List<MemTable> tablesToFlushDirectly = new ArrayList<>();

            rwLock.readLock().lock();
            try {
                for (MemTable imm : immutableMemTables) {
                    MemTable.State st = imm.getState();
                    if (st == MemTable.State.FLUSHING) {
                        if (imm.getFlushFuture() != null) {
                            inFlightFutures.add(imm.getFlushFuture());
                        }
                    } else if (st == MemTable.State.QUEUED || st == MemTable.State.SEALED || st == MemTable.State.FAILED) {
                        tablesToFlushDirectly.add(imm);
                    }
                }
            } finally {
                rwLock.readLock().unlock();
            }

            // Directly flush any un-flushed immutable MemTables (QUEUED, SEALED, FAILED)
            for (MemTable imm : tablesToFlushDirectly) {
                CompletableFuture<SSTableReader> attemptFuture = imm.claimFlushAttempt();
                if (attemptFuture != null) {
                    try {
                        long tableId = sstableIdGenerator.getAndIncrement();
                        MemTableFlushTask flushTask = new MemTableFlushTask(
                                imm, sstableDir, tableId, config, imm.getAssociatedWalFile());
                        SSTableReader newReader = flushTask.execute();

                        rwLock.writeLock().lock();
                        try {
                            LifecycleState state = lifecycleState.get();
                            if (state != LifecycleState.OPEN) {
                                newReader.close();
                                imm.setState(MemTable.State.FAILED);
                                attemptFuture.completeExceptionally(new IOException("Engine " + state + " during flush publication"));
                                return;
                            }
                            sstables.add(0, newReader);
                            immutableMemTables.remove(imm);
                            imm.setState(MemTable.State.INSTALLED);
                            flushTask.retireAssociatedWal();
                            attemptFuture.complete(newReader);
                        } finally {
                            rwLock.writeLock().unlock();
                        }
                    } catch (Exception e) {
                        imm.setState(MemTable.State.FAILED);
                        attemptFuture.completeExceptionally(e);
                        throw new IOException("Manual flush of immutable MemTable failed: " + e.getMessage(), e);
                    }
                } else if (imm.getFlushFuture() != null) {
                    inFlightFutures.add(imm.getFlushFuture());
                }
            }

            // Await any in-flight background flushes outside of locks
            for (CompletableFuture<SSTableReader> f : inFlightFutures) {
                try {
                    f.get(30, TimeUnit.SECONDS);
                } catch (Exception e) {
                    throw new IOException("Awaiting background flush completion failed: " + e.getMessage(), e);
                }
            }

            // Seal and flush active MemTable if non-empty
            MemTable tableToFlush = null;
            File rotatedWal = null;
            CompletableFuture<SSTableReader> activeFuture = null;

            rwLock.writeLock().lock();
            try {
                if (lifecycleState.get() != LifecycleState.OPEN) {
                    throw new IllegalStateException("Storage engine is " + lifecycleState.get());
                }
                if (!activeMemTable.isEmpty()) {
                    long nextSegId = walSegmentIdGenerator.getAndIncrement();
                    rotatedWal = wal.rotate(nextSegId);
                    tableToFlush = activeMemTable;
                    tableToFlush.setState(MemTable.State.SEALED);
                    tableToFlush.setAssociatedWalFile(rotatedWal);
                    activeFuture = tableToFlush.claimFlushAttempt();
                    activeMemTable = new MemTable(System.currentTimeMillis());
                    immutableMemTables.add(tableToFlush);
                }
            } catch (IOException e) {
                this.failureReason = "WAL rotation failed during manual flush: " + e.getMessage();
                this.lifecycleState.set(LifecycleState.FAILED);
                throw e;
            } finally {
                rwLock.writeLock().unlock();
            }

            if (tableToFlush != null && activeFuture != null) {
                try {
                    long tableId = sstableIdGenerator.getAndIncrement();
                    MemTableFlushTask flushTask = new MemTableFlushTask(
                            tableToFlush, sstableDir, tableId, config, rotatedWal);
                    SSTableReader newReader = flushTask.execute();

                    rwLock.writeLock().lock();
                    try {
                        LifecycleState state = lifecycleState.get();
                        if (state != LifecycleState.OPEN) {
                            newReader.close();
                            tableToFlush.setState(MemTable.State.FAILED);
                            activeFuture.completeExceptionally(new IOException("Engine " + state + " during flush publication"));
                            return;
                        }
                        sstables.add(0, newReader);
                        immutableMemTables.remove(tableToFlush);
                        tableToFlush.setState(MemTable.State.INSTALLED);
                        flushTask.retireAssociatedWal();
                        activeFuture.complete(newReader);
                    } finally {
                        rwLock.writeLock().unlock();
                    }
                } catch (Exception e) {
                    tableToFlush.setState(MemTable.State.FAILED);
                    activeFuture.completeExceptionally(e);
                    throw new IOException("Manual flush of active MemTable failed: " + e.getMessage(), e);
                }
            }
        } finally {
            exitOperation();
        }
    }

    @Override
    public void retryFailedFlushes() throws IOException {
        enterOperation();
        try {
            rwLock.writeLock().lock();
            try {
                if (lifecycleState.get() == LifecycleState.CLOSING || lifecycleState.get() == LifecycleState.CLOSED) {
                    throw new IllegalStateException("Storage engine is " + lifecycleState.get());
                }
                if (lifecycleState.get() == LifecycleState.FAILED) {
                    if (wal == null || wal.isClosed()) {
                        throw new IOException("Cannot recover engine from FAILED state: WAL is null or closed");
                    }
                    // Test WAL directory writeability with a test probe file
                    File probeFile = new File(walDir, ".recovery_probe_" + System.nanoTime() + ".tmp");
                    try {
                        if (!probeFile.createNewFile()) {
                            throw new IOException("Cannot create probe file in WAL directory " + walDir);
                        }
                        probeFile.delete();
                    } catch (IOException probeEx) {
                        throw new IOException("Cannot recover engine from FAILED state: WAL storage is not writable: " + probeEx.getMessage(), probeEx);
                    }
                    // Recovered safely
                    lifecycleState.set(LifecycleState.OPEN);
                    failureReason = null;
                }
            } finally {
                rwLock.writeLock().unlock();
            }

            flushMemTable();
        } finally {
            exitOperation();
        }
    }

    @Override
    public synchronized void compact() throws IOException {
        enterOperation();
        try {
            if (lifecycleState.get() != LifecycleState.OPEN) return;

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

            boolean hasPendingFlushes = !immutableMemTables.isEmpty() || activeFlushTasks.get() > 0;
            boolean isFullCompaction = (tablesToCompact.size() == sstables.size());
            boolean canPruneTombstones = isFullCompaction && !hasPendingFlushes;

            SSTableReader compacted = CompactionEngine.compact(
                    tablesToCompact,
                    targetFile,
                    newTableId,
                    1, // Level 1
                    config.getSSTableBlockSize(),
                    config.getBloomFpr(),
                    canPruneTombstones
            );

            // Atomic publication transaction:
            rwLock.writeLock().lock();
            try {
                LifecycleState state = lifecycleState.get();
                if (state != LifecycleState.OPEN) {
                    // Engine closed or closing while compacting; discard compacted reader safely
                    compacted.close();
                    return;
                }

                sstables.removeAll(tablesToCompact);
                sstables.add(compacted);
                sstables.sort((a, b) -> Long.compare(b.getTableId(), a.getTableId()));

                for (SSTableReader oldReader : tablesToCompact) {
                    oldReader.markObsolete();
                }
            } finally {
                rwLock.writeLock().unlock();
            }
        } finally {
            exitOperation();
        }
    }

    @Override
    public void registerTableSchema(Schema schema) {
        if (schema == null) {
            throw new IllegalArgumentException("Schema cannot be null");
        }
        registerTableSchema(schema.getTableName(), schema);
    }

    @Override
    public void registerTableSchema(String tableName, Schema schema) {
        if (tableName == null || !TABLE_NAME_PATTERN.matcher(tableName).matches()) {
            throw new IllegalArgumentException("Invalid table name: '" + tableName + "'. Must be alphanumeric and up to 64 chars.");
        }
        if (schema == null) {
            throw new IllegalArgumentException("Schema cannot be null");
        }
        if (!tableName.equals(schema.getTableName())) {
            throw new IllegalArgumentException("Table name '" + tableName + "' does not match schema table name '" + schema.getTableName() + "'");
        }
        if (schema.getColumnCount() == 0) {
            throw new IllegalArgumentException("Schema must define at least one column");
        }

        // Check against existing registered schema
        Schema existing = tableSchemas.get(tableName);
        if (existing != null) {
            if (existing.equals(schema)) {
                return; // Idempotent re-registration of identical schema
            }
            throw new IllegalArgumentException("Cannot replace existing schema for table '" + tableName 
                    + "': existing=" + existing + ", new=" + schema);
        }

        // Check compatibility against already loaded columnar chunks
        List<ColumnarChunk> existingChunks = columnarTables.get(tableName);
        if (existingChunks != null && !existingChunks.isEmpty()) {
            for (ColumnarChunk chunk : existingChunks) {
                Schema chunkSchema = chunk.getSchema();
                if (!chunkSchema.equals(schema)) {
                    throw new IllegalArgumentException("New schema incompatible with existing chunk data for table '" 
                            + tableName + "': chunkSchema=" + chunkSchema + ", newSchema=" + schema);
                }
            }
        }

        tableSchemas.put(tableName, schema);
    }

    @Override
    public Schema getTableSchema(String tableName) {
        if (tableName == null) return null;
        return tableSchemas.get(tableName);
    }

    @Override
    public void ingestColumnar(String tableName, List<Map<String, ColumnValue>> records) throws IOException {
        enterOperation();
        try {
            LifecycleState state = lifecycleState.get();
            if (state == LifecycleState.CLOSING || state == LifecycleState.CLOSED) {
                throw new IllegalStateException("Storage engine is closing or closed");
            }
            if (state == LifecycleState.FAILED) {
                throw new IllegalStateException("Storage engine is in FAILED state: " + failureReason);
            }
            if (tableName == null || !TABLE_NAME_PATTERN.matcher(tableName).matches()) {
                throw new IllegalArgumentException("Invalid table name: '" + tableName + "'. Must be alphanumeric and up to 64 chars.");
            }
            if (records == null || records.isEmpty()) return;

            // Ensure stable table schema across all batches; rejects all-null columns if untyped
            Schema schema = getOrInferTableSchema(tableName, records);

            // Partition ingestion into batches of at most config.getColumnarMaxRows()
            int maxRowsPerChunk = Math.max(1, config.getColumnarMaxRows());
            for (int i = 0; i < records.size(); i += maxRowsPerChunk) {
                int toIndex = Math.min(i + maxRowsPerChunk, records.size());
                List<Map<String, ColumnValue>> subBatch = records.subList(i, toIndex);
                ingestColumnarChunkWithSchema(tableName, schema, subBatch);
            }
        } finally {
            exitOperation();
        }
    }

    private Schema getOrInferTableSchema(String tableName, List<Map<String, ColumnValue>> records) {
        Schema existing = tableSchemas.get(tableName);
        if (existing != null) {
            return existing;
        }

        Map<String, ColumnType> colTypes = new LinkedHashMap<>();
        // Scan all rows to find non-null types
        for (Map<String, ColumnValue> row : records) {
            for (Map.Entry<String, ColumnValue> entry : row.entrySet()) {
                String col = entry.getKey();
                ColumnValue val = entry.getValue();
                if (!colTypes.containsKey(col)) {
                    if (val != null && !val.isNull()) {
                        colTypes.put(col, val.getType());
                    }
                } else if (colTypes.get(col) == null && val != null && !val.isNull()) {
                    colTypes.put(col, val.getType());
                }
            }
        }

        // Check for columns where all values in batch are untyped null
        for (Map<String, ColumnValue> row : records) {
            for (String col : row.keySet()) {
                if (!colTypes.containsKey(col) || colTypes.get(col) == null) {
                    throw new IllegalArgumentException("Cannot infer column type for '" + col
                            + "' in table '" + tableName + "': all values in batch are untyped null. "
                            + "Register table schema explicitly before ingesting all-null columns.");
                }
            }
        }

        List<ColumnDefinition> colDefs = new ArrayList<>();
        for (Map.Entry<String, ColumnType> entry : colTypes.entrySet()) {
            colDefs.add(new ColumnDefinition(entry.getKey(), entry.getValue()));
        }
        Schema schema = new Schema(tableName, colDefs);
        tableSchemas.putIfAbsent(tableName, schema);
        return tableSchemas.get(tableName);
    }

    private void ingestColumnarChunkWithSchema(String tableName, Schema schema, List<Map<String, ColumnValue>> records) throws IOException {
        List<ColumnDefinition> colDefs = schema.getColumnDefinitions();
        Set<String> knownColumns = new HashSet<>();
        Map<String, ColumnType> expectedTypes = new HashMap<>();
        for (ColumnDefinition col : colDefs) {
            knownColumns.add(col.getName());
            expectedTypes.put(col.getName(), col.getType());
        }

        Map<String, List<ColumnValue>> colData = new HashMap<>();
        for (ColumnDefinition col : colDefs) {
            colData.put(col.getName(), new ArrayList<>(records.size()));
        }

        // Validate all records before encoding
        for (Map<String, ColumnValue> row : records) {
            for (String rowCol : row.keySet()) {
                if (!knownColumns.contains(rowCol)) {
                    throw new IllegalArgumentException("Unexpected column '" + rowCol + "' not in table schema: " + knownColumns);
                }
            }

            for (ColumnDefinition colDef : colDefs) {
                String colName = colDef.getName();
                ColumnValue val = row.get(colName);
                if (val == null || val.isNull()) {
                    colData.get(colName).add(ColumnValue.ofNull(colDef.getType()));
                } else {
                    if (val.getType() != null && val.getType() != colDef.getType()) {
                        throw new IllegalArgumentException("Type mismatch for column '" + colName
                                + "': expected " + colDef.getType() + ", but got " + val.getType());
                    }
                    colData.get(colName).add(val);
                }
            }
        }

        // Compute Zone Maps
        Map<String, ZoneMap> zoneMaps = new HashMap<>();
        for (ColumnDefinition col : colDefs) {
            ZoneMap zm = new ZoneMap(col.getType());
            for (ColumnValue val : colData.get(col.getName())) {
                zm.update(val);
            }
            zoneMaps.put(col.getName(), zm);
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
        return chunks != null ? Collections.unmodifiableList(chunks) : Collections.emptyList();
    }

    @Override
    public Map<String, Object> getMetrics() {
        Map<String, Object> metrics = new LinkedHashMap<>();
        metrics.put("lifecycleState", lifecycleState.get().name());
        metrics.put("failureReason", failureReason != null ? failureReason : "none");
        metrics.put("activeOperations", activeOperations.get());
        metrics.put("totalPuts", totalPuts.get());
        metrics.put("totalGets", totalGets.get());
        metrics.put("totalDeletes", totalDeletes.get());
        metrics.put("memtableHits", memtableHits.get());
        metrics.put("sstableHits", sstableHits.get());
        metrics.put("bloomFilterMisses", bloomFilterMisses.get());
        metrics.put("activeMemTableRecords", activeMemTable != null ? activeMemTable.getRecordCount() : 0);
        metrics.put("activeMemTableBytes", activeMemTable != null ? activeMemTable.getByteSize() : 0);
        metrics.put("immutableMemTablesCount", immutableMemTables.size());
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
        if (!lifecycleState.compareAndSet(LifecycleState.OPEN, LifecycleState.CLOSING)
                && lifecycleState.get() != LifecycleState.CLOSING
                && lifecycleState.get() != LifecycleState.FAILED) {
            return; // Already closed
        }

        // 1. Await admitted active operations to finish (up to 5s)
        long deadline = System.currentTimeMillis() + 5000;
        while (activeOperations.get() > 0 && System.currentTimeMillis() < deadline) {
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        // 2. Stop background execution
        backgroundExecutor.shutdown();
        boolean terminated = false;
        try {
            terminated = backgroundExecutor.awaitTermination(5, TimeUnit.SECONDS);
            if (!terminated) {
                backgroundExecutor.shutdownNow();
                terminated = backgroundExecutor.awaitTermination(5, TimeUnit.SECONDS);
            }
        } catch (InterruptedException e) {
            backgroundExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        if (!terminated || activeOperations.get() > 0) {
            // Do NOT close resources in use! Leave in CLOSING state and report failure clearly.
            throw new IOException("Storage operations (" + activeOperations.get() + " active) or background workers did not terminate within bounded timeout; resources kept open. Retry close() after workers stop.");
        }

        rwLock.writeLock().lock();
        try {
            lifecycleState.set(LifecycleState.CLOSED);
            if (wal != null) {
                wal.close();
            }
            for (SSTableReader reader : sstables) {
                try {
                    reader.close();
                } catch (Throwable ignored) {}
            }
        } finally {
            rwLock.writeLock().unlock();
        }
    }
}
