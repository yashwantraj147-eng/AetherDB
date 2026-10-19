package com.aetherdb.storage.memtable;

import com.aetherdb.common.AetherConfig;
import com.aetherdb.common.Constants;
import com.aetherdb.storage.sstable.SSTableMetadata;
import com.aetherdb.storage.sstable.SSTableReader;
import com.aetherdb.storage.sstable.SSTableWriter;

import java.io.File;
import java.io.IOException;

/**
 * Task that flushes a sealed immutable MemTable into an on-disk Level-0 SSTable.
 */
public class MemTableFlushTask {

    private final MemTable memTable;
    private final File sstableDir;
    private final long tableId;
    private final AetherConfig config;

    public MemTableFlushTask(MemTable memTable, File sstableDir, long tableId, AetherConfig config) {
        this.memTable = memTable;
        this.sstableDir = sstableDir;
        this.tableId = tableId;
        this.config = config;
    }

    /**
     * Executes flush to disk and returns an open SSTableReader for the new table.
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
        return new SSTableReader(sstableFile);
    }
}
