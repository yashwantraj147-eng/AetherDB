package com.aetherdb.partition;

import com.aetherdb.common.Constants;
import com.aetherdb.common.MurmurHash3;

import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Consistent Hash Ring Router using MurmurHash3 and Virtual Nodes (vnodes).
 * Guarantees uniform partition distribution and minimal remapping on topology changes.
 */
public class ConsistentHashRouter {

    private final int vnodesPerNode;
    private final TreeMap<Integer, VirtualNode> ring = new TreeMap<>();
    private final Set<String> physicalNodes = new HashSet<>();
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();

    public ConsistentHashRouter(int vnodesPerNode) {
        this.vnodesPerNode = Math.max(1, vnodesPerNode);
    }

    public ConsistentHashRouter() {
        this(Constants.DEFAULT_VNODES);
    }

    /**
     * Adds a physical node to the ring, allocating virtual nodes.
     */
    public void addNode(String nodeId) {
        rwLock.writeLock().lock();
        try {
            if (physicalNodes.add(nodeId)) {
                for (int i = 0; i < vnodesPerNode; i++) {
                    VirtualNode vNode = new VirtualNode(nodeId, i);
                    int hash = MurmurHash3.hash32(vNode.getKey());
                    ring.put(hash, vNode);
                }
            }
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    /**
     * Removes a physical node and its virtual nodes from the ring.
     */
    public void removeNode(String nodeId) {
        rwLock.writeLock().lock();
        try {
            if (physicalNodes.remove(nodeId)) {
                for (int i = 0; i < vnodesPerNode; i++) {
                    VirtualNode vNode = new VirtualNode(nodeId, i);
                    int hash = MurmurHash3.hash32(vNode.getKey());
                    ring.remove(hash);
                }
            }
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    /**
     * Routes a key to its assigned physical node on the ring.
     */
    public String routeKey(byte[] key) {
        rwLock.readLock().lock();
        try {
            if (ring.isEmpty()) {
                throw new IllegalStateException("Consistent hash ring is empty");
            }

            int hash = MurmurHash3.hash32(key);
            Map.Entry<Integer, VirtualNode> entry = ring.ceilingEntry(hash);
            if (entry == null) {
                // Wrap around the circle
                entry = ring.firstEntry();
            }

            return entry.getValue().getPhysicalNodeId();
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public String routeKey(String key) {
        return routeKey(key.getBytes(Constants.DEFAULT_CHARSET));
    }

    public int getRingSize() {
        rwLock.readLock().lock();
        try {
            return ring.size();
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public Set<String> getPhysicalNodes() {
        rwLock.readLock().lock();
        try {
            return Collections.unmodifiableSet(new HashSet<>(physicalNodes));
        } finally {
            rwLock.readLock().unlock();
        }
    }
}
