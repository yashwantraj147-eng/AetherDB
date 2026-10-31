package com.aetherdb.storage.engine;

/**
 * Explicit lifecycle states for the AetherDB storage engine.
 */
public enum LifecycleState {
    /**
     * Storage engine is fully operational and accepting reads and writes.
     */
    OPEN,

    /**
     * Storage engine is gracefully terminating; active operations are completing,
     * new writes are rejected, and background workers are awaiting termination.
     */
    CLOSING,

    /**
     * Storage engine has completed shutdown; all resources (WAL, SSTables) are closed.
     */
    CLOSED,

    /**
     * Storage engine encountered an unrecoverable failure (e.g., WAL rotation failure).
     * Rejects new writes to protect data durability until recovered or restarted.
     */
    FAILED
}
