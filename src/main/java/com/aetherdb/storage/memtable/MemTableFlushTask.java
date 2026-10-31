package com.aetherdb.storage.memtable;

import com.aetherdb.common.AetherConfig;
import com.aetherdb.common.Constants;
import com.aetherdb.storage.sstable.SSTableReader;
import com.aetherdb.storage.sstable.SSTableWriter;

import java.io.File;
import java.io.IOException;

/**
 * Task that flushes a sealed immutable MemTable into an on-disk Level-0 SSTable.
 * Coordinates crash-safe SSTable writing, validation, and WAL segment retirement.
 */
public class MemTableFlushTask {

    private final MemTable memTable;
    private final File sstableDir;
    private final long tableId;
    private final AetherConfig config;
    private final File associatedWalFile;

    public MemTableFlushTask(MemTable memTable, File sstableDir, long tableId, AetherConfig config) {
        this(memTable, sstableDir, tableId, config, null);
    }

    public MemTableFlushTask(MemTable memTable, File sstableDir, long tableId, AetherConfig config, File associatedWalFile) {
        this.memTable = memTable;
        this.sstableDir = sstableDir;
        this.tableId = tableId;
        this.config = config;
        this.associatedWalFile = associatedWalFile;
    }

    /**
     * Executes flush to disk via atomic temporary file installation,
     * returns an open SSTableReader for the new table, and retires the associated WAL segment.
     */
    public SSTableReader execute() throws IOException {
        String filename = String.format("sstable_%06d%s", tableId, Constants.SSTABLE_DATA_EXT);
        File sstableFile = new File(sstableDir, filename);

        SSTableWriter writer = new SSTableWriter(
                sstableFile,
                tableId,
                0, // Level 0
                config.getSSTableBlockSize(),
                config.getBloomFpr()
        );

        writer.write(memTable.iterator(), memTable.size());
        SSTableReader reader = new SSTableReader(sstableFile);

        // Retire obsolete WAL file now that SSTable is durable and valid
        if (associatedWalFile != null && associatedWalFile.exists()) {
            associatedWalFile.delete();
        }

        return reader;
    }

    public File getAssociatedWalFile() {
        return associatedWalFile;
    }
}
