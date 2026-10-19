package com.aetherdb;

import com.aetherdb.common.AetherConfig;
import com.aetherdb.consensus.raft.RaftNode;
import com.aetherdb.network.HttpApiServer;
import com.aetherdb.partition.ConsistentHashRouter;
import com.aetherdb.storage.engine.HybridStorageManager;

import java.io.File;

/**
 * Main daemon bootstrap entrypoint for AetherDB storage node.
 */
public class AetherServer {

    public static void main(String[] args) {
        String configFile = args.length > 0 ? args[0] : "config/aether-cluster.properties";

        printBanner();

        try {
            System.out.println(">> Loading configuration from: " + configFile);
            AetherConfig config = AetherConfig.loadFromFile(configFile);

            System.out.println(">> Initializing Hybrid Storage Engine (LSM + Columnar)...");
            HybridStorageManager storageManager = new HybridStorageManager(config);

            System.out.println(">> Initializing Raft Consensus Engine (Node: " + config.getNodeId() + ")...");
            RaftNode raftNode = new RaftNode(config, storageManager);

            System.out.println(">> Setting up Consistent Hash Ring Router...");
            ConsistentHashRouter router = new ConsistentHashRouter(config.getVNodesPerNode());
            router.addNode(config.getNodeId());
            for (AetherConfig.PeerNode peer : config.getClusterPeers()) {
                router.addNode(peer.getId());
            }

            System.out.println(">> Launching Virtual-Thread HTTP Gateway on " + config.getNodeHost() + ":" + config.getNodePort() + "...");
            HttpApiServer server = new HttpApiServer(config, storageManager, raftNode, router);
            server.start();

            // Register JVM shutdown hook for clean resource reclamation
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("\n>> Shutting down AetherDB node gracefully...");
                try {
                    server.close();
                    raftNode.close();
                    storageManager.close();
                    System.out.println(">> AetherDB node shut down successfully.");
                } catch (Exception e) {
                    System.err.println("Shutdown error: " + e.getMessage());
                }
            }));

            System.out.println("\n========================================================");
            System.out.println("  AetherDB Node [" + config.getNodeId() + "] is ONLINE and READY!");
            System.out.println("  Web Dashboard: http://" + config.getNodeHost() + ":" + config.getNodePort() + "/dashboard");
            System.out.println("  REST Endpoint: http://" + config.getNodeHost() + ":" + config.getNodePort() + "/api/v1/metrics");
            System.out.println("========================================================\n");

        } catch (Exception e) {
            System.err.println("FATAL: Failed to start AetherDB: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static void printBanner() {
        System.out.println("""
                 ___        _   _              ____  ____ \s
                / _ \\      | | | |            |  _ \\|  _ \\\s
               / /_\\ \\ ___ | |_| |__   ___ _ _| | | | |_) |
               |  _  |/ _ \\| __| '_ \\ / _ \\ '__| | | |  _ <\s
               | | | |  __/| |_| | | |  __/ |  | |_| | |_) |
               \\_| |_/\\___| \\__|_| |_|\\___|_|  |____/|____/\s
               High-Performance Distributed Storage Engine [Java 26]
        """);
    }
}
