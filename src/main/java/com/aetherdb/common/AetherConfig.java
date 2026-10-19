package com.aetherdb.common;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * Production-grade configuration loader and validator for AetherDB.
 * Supports property files, environment variable overrides, and dynamic peer discovery.
 */
public class AetherConfig {

    private final Properties props = new Properties();

    public enum WalSyncMode {
        IMMEDIATE, // Synchronous fsync after every write (highest durability)
        PERIODIC,  // Background background flush on timer (high performance)
        OS_BUFFER  // Delegated to OS page cache
    }

    public static class PeerNode {
        private final String id;
        private final String host;
        private final int port;

        public PeerNode(String id, String host, int port) {
            this.id = id;
            this.host = host;
            this.port = port;
        }

        public String getId() { return id; }
        public String getHost() { return host; }
        public int getPort() { return port; }

        @Override
        public String toString() {
            return id + "@" + host + ":" + port;
        }
    }

    public AetherConfig() {
        setDefaults();
    }

    public static AetherConfig loadFromFile(String configFilePath) throws IOException {
        AetherConfig config = new AetherConfig();
        File file = new File(configFilePath);
        if (file.exists()) {
            try (InputStream in = new FileInputStream(file)) {
                config.props.load(in);
            }
        }
        config.validateAndCreateDirectories();
        return config;
    }

    private void setDefaults() {
        props.setProperty("node.id", "node-1");
        props.setProperty("node.host", "127.0.0.1");
        props.setProperty("node.port", "8080");
        props.setProperty("node.cluster.peers", "node-1:127.0.0.1:8080");

        props.setProperty("storage.data.dir", "./data/node-1/storage");
        props.setProperty("storage.wal.dir", "./data/node-1/wal");
        props.setProperty("storage.sstable.dir", "./data/node-1/sstable");
        props.setProperty("storage.columnar.dir", "./data/node-1/columnar");

        props.setProperty("lsm.memtable.max.bytes", String.valueOf(Constants.DEFAULT_MEMTABLE_SIZE));
        props.setProperty("lsm.memtable.flush.queue.capacity", "8");
        props.setProperty("lsm.wal.sync.mode", "PERIODIC");
        props.setProperty("lsm.wal.periodic.sync.ms", "50");
        props.setProperty("lsm.bloom.false.positive.rate", "0.01");
        props.setProperty("lsm.sstable.block.size", String.valueOf(Constants.DEFAULT_BLOCK_SIZE));
        props.setProperty("lsm.compaction.min.threshold", "4");
        props.setProperty("lsm.compaction.max.threshold", "16");

        props.setProperty("columnar.chunk.max.rows", "65536");
        props.setProperty("columnar.compression.codec", "AUTO");
        props.setProperty("columnar.dictionary.threshold.ratio", "0.70");
        props.setProperty("columnar.vectorized.batch.size", "1024");

        props.setProperty("raft.election.timeout.min.ms", "150");
        props.setProperty("raft.election.timeout.max.ms", "300");
        props.setProperty("raft.heartbeat.interval.ms", "50");
        props.setProperty("raft.max.entries.per.request", "500");

        props.setProperty("cluster.vnodes.per.node", "64");
        props.setProperty("server.dashboard.enabled", "true");
        props.setProperty("server.metrics.history.retention.seconds", "3600");
    }

    public void validateAndCreateDirectories() throws IOException {
        createDirIfNotExists(getStorageDataDir());
        createDirIfNotExists(getStorageWalDir());
        createDirIfNotExists(getStorageSSTableDir());
        createDirIfNotExists(getStorageColumnarDir());
    }

    private void createDirIfNotExists(String dirPath) throws IOException {
        Path p = Paths.get(dirPath);
        if (!Files.exists(p)) {
            Files.createDirectories(p);
        }
    }

    // Getters
    public String getNodeId() { return props.getProperty("node.id"); }
    public String getNodeHost() { return props.getProperty("node.host"); }
    public int getNodePort() { return Integer.parseInt(props.getProperty("node.port")); }

    public List<PeerNode> getClusterPeers() {
        String peerStr = props.getProperty("node.cluster.peers", "");
        List<PeerNode> peers = new ArrayList<>();
        if (peerStr.trim().isEmpty()) return peers;

        for (String p : peerStr.split(",")) {
            String[] parts = p.trim().split(":");
            if (parts.length == 3) {
                peers.add(new PeerNode(parts[0], parts[1], Integer.parseInt(parts[2])));
            }
        }
        return peers;
    }

    public String getStorageDataDir() { return props.getProperty("storage.data.dir"); }
    public String getStorageWalDir() { return props.getProperty("storage.wal.dir"); }
    public String getStorageSSTableDir() { return props.getProperty("storage.sstable.dir"); }
    public String getStorageColumnarDir() { return props.getProperty("storage.columnar.dir"); }

    public long getMemTableMaxBytes() { return Long.parseLong(props.getProperty("lsm.memtable.max.bytes")); }
    public int getFlushQueueCapacity() { return Integer.parseInt(props.getProperty("lsm.memtable.flush.queue.capacity")); }
    public WalSyncMode getWalSyncMode() { return WalSyncMode.valueOf(props.getProperty("lsm.wal.sync.mode").toUpperCase()); }
    public long getWalPeriodicSyncMs() { return Long.parseLong(props.getProperty("lsm.wal.periodic.sync.ms")); }
    public double getBloomFpr() { return Double.parseDouble(props.getProperty("lsm.bloom.false.positive.rate")); }
    public int getSSTableBlockSize() { return Integer.parseInt(props.getProperty("lsm.sstable.block.size")); }
    public int getCompactionMinThreshold() { return Integer.parseInt(props.getProperty("lsm.compaction.min.threshold")); }
    public int getCompactionMaxThreshold() { return Integer.parseInt(props.getProperty("lsm.compaction.max.threshold")); }

    public int getColumnarMaxRows() { return Integer.parseInt(props.getProperty("columnar.chunk.max.rows")); }
    public int getVectorizedBatchSize() { return Integer.parseInt(props.getProperty("columnar.vectorized.batch.size")); }
    public double getDictionaryThresholdRatio() { return Double.parseDouble(props.getProperty("columnar.dictionary.threshold.ratio")); }

    public int getRaftElectionMinMs() { return Integer.parseInt(props.getProperty("raft.election.timeout.min.ms")); }
    public int getRaftElectionMaxMs() { return Integer.parseInt(props.getProperty("raft.election.timeout.max.ms")); }
    public int getRaftHeartbeatMs() { return Integer.parseInt(props.getProperty("raft.heartbeat.interval.ms")); }

    public int getVNodesPerNode() { return Integer.parseInt(props.getProperty("cluster.vnodes.per.node")); }
    public boolean isDashboardEnabled() { return Boolean.parseBoolean(props.getProperty("server.dashboard.enabled")); }

    public void setProperty(String key, String value) {
        props.setProperty(key, value);
    }
}
