package com.aetherdb.partition;

import com.aetherdb.common.Constants;
import com.aetherdb.common.MurmurHash3;

import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Consistent Hash Ring Router using MurmurHash3 and Virtual Nodes (vnodes).
 * Guarantees uniform partition distribution and minimal remapping on topology changes.
 *
 * Implements collision-safe virtual node storage (resolving hash collisions without overwriting)
 * and thread-safe routing.
 *
 * Integration note: The router currently serves as a cluster routing and partitioning component.
 * Local KV storage engine writes and reads operate on the local node.
 */
public class ConsistentHashRouter {

    private final int vnodesPerNode;
    private final TreeMap<Integer, List<VirtualNode>> ring = new TreeMap<>();
    private final Set<String> physicalNodes = new HashSet<>();
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();

    public ConsistentHashRouter(int vnodesPerNode) {
        this.vnodesPerNode = Math.max(1, vnodesPerNode);
    }

    public ConsistentHashRouter() {
        this(Constants.DEFAULT_VNODES);
    }

    /**
     * Adds a physical node to the ring, allocating collision-safe virtual nodes.
     */
    public void addNode(String nodeId) {
        if (nodeId == null) return;
        rwLock.writeLock().lock();
        try {
            if (physicalNodes.add(nodeId)) {
                for (int i = 0; i < vnodesPerNode; i++) {
                    VirtualNode vNode = new VirtualNode(nodeId, i);
                    int hash = MurmurHash3.hash32(vNode.getKey());
                    ring.computeIfAbsent(hash, k -> new ArrayList<>()).add(vNode);
                }
            }
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    /**
     * Removes a physical node and its virtual nodes from the ring safely.
     */
    public void removeNode(String nodeId) {
        if (nodeId == null) return;
        rwLock.writeLock().lock();
        try {
            if (physicalNodes.remove(nodeId)) {
                for (int i = 0; i < vnodesPerNode; i++) {
                    final int replicaIdx = i;
                    VirtualNode vNode = new VirtualNode(nodeId, i);
                    int hash = MurmurHash3.hash32(vNode.getKey());
                    List<VirtualNode> list = ring.get(hash);
                    if (list != null) {
                        list.removeIf(vn -> vn.getPhysicalNodeId().equals(nodeId) && vn.getReplicaIndex() == replicaIdx);
                        if (list.isEmpty()) {
                            ring.remove(hash);
                        }
                    }
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
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }
        rwLock.readLock().lock();
        try {
            if (ring.isEmpty()) {
                throw new IllegalStateException("Consistent hash ring is empty");
            }

            int hash = MurmurHash3.hash32(key);
            Map.Entry<Integer, List<VirtualNode>> entry = ring.ceilingEntry(hash);
            if (entry == null) {
                // Wrap around the circle
                entry = ring.firstEntry();
            }

            return entry.getValue().get(0).getPhysicalNodeId();
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public String routeKey(String key) {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }
        return routeKey(key.getBytes(Constants.DEFAULT_CHARSET));
    }

    /**
     * Total number of virtual nodes on the ring.
     */
    public int getRingSize() {
        rwLock.readLock().lock();
        try {
            int count = 0;
            for (List<VirtualNode> list : ring.values()) {
                count += list.size();
            }
            return count;
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

    public void clear() {
        rwLock.writeLock().lock();
        try {
            ring.clear();
            physicalNodes.clear();
        } finally {
            rwLock.writeLock().unlock();
        }
    }
}
