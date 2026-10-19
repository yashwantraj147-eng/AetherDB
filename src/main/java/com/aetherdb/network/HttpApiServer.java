package com.aetherdb.network;

import com.aetherdb.common.AetherConfig;
import com.aetherdb.consensus.raft.RaftNode;
import com.aetherdb.partition.ConsistentHashRouter;
import com.aetherdb.storage.engine.StorageEngine;
import com.sun.net.httpserver.HttpServer;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

/**
 * High-performance embedded HTTP Gateway for AetherDB powered by Java 26 Virtual Threads.
 * Supports tens of thousands of concurrent client connections with zero external dependencies.
 */
public class HttpApiServer implements AutoCloseable {

    private final HttpServer server;
    private final int port;

    public HttpApiServer(AetherConfig config, StorageEngine storageEngine,
                         RaftNode raftNode, ConsistentHashRouter router) throws IOException {
        this.port = config.getNodePort();
        this.server = HttpServer.create(new InetSocketAddress(config.getNodeHost(), port), 0);

        // Bind virtual thread executor for lightweight non-blocking connection handling
        this.server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());

        RestApiHandler restHandler = new RestApiHandler(storageEngine, raftNode, router);
        server.createContext("/api/", restHandler);

        if (config.isDashboardEnabled()) {
            File dashboardFile = new File("web/dashboard.html");
            if (!dashboardFile.exists()) {
                dashboardFile = new File("bin/web/dashboard.html");
            }
            server.createContext("/dashboard", new DashboardHandler(dashboardFile));
            server.createContext("/", new DashboardHandler(dashboardFile));
        }
    }

    public void start() {
        server.start();
        System.out.println("⚡ AetherDB HTTP Server listening on port " + port + " (Virtual Threads Enabled)");
    }

    @Override
    public void close() {
        server.stop(1);
    }
}
