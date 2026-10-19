package com.aetherdb.partition;

import java.util.Objects;

/**
 * Virtual node descriptor on the consistent hash ring.
 */
public class VirtualNode {

    private final String physicalNodeId;
    private final int replicaIndex;

    public VirtualNode(String physicalNodeId, int replicaIndex) {
        this.physicalNodeId = physicalNodeId;
        this.replicaIndex = replicaIndex;
    }

    public String getPhysicalNodeId() {
        return physicalNodeId;
    }

    public int getReplicaIndex() {
        return replicaIndex;
    }

    public String getKey() {
        return physicalNodeId + "#VN#" + replicaIndex;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        VirtualNode that = (VirtualNode) o;
        return replicaIndex == that.replicaIndex && Objects.equals(physicalNodeId, that.physicalNodeId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(physicalNodeId, replicaIndex);
    }

    @Override
    public String toString() {
        return getKey();
    }
}
