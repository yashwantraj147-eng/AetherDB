package com.aetherdb.consensus.raft;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Persistent and volatile state tracking for a Raft consensus node.
 */
public class RaftState {

    // Persistent state on all servers
    private volatile long currentTerm = 0;
    private volatile String votedFor = null;
    private final List<LogPayload> log = new CopyOnWriteArrayList<>();

    // Volatile state on all servers
    private volatile long commitIndex = 0;
    private volatile long lastApplied = 0;

    // Volatile state on leaders (reinitialized after election)
    private final Map<String, Long> nextIndex = new ConcurrentHashMap<>();
    private final Map<String, Long> matchIndex = new ConcurrentHashMap<>();

    public RaftState() {
        // Log is 1-indexed in Raft; insert dummy entry at index 0
        log.add(new LogPayload(0, 0, null, null, null, null));
    }

    public long getCurrentTerm() { return currentTerm; }
    public void setCurrentTerm(long term) { this.currentTerm = term; }

    public String getVotedFor() { return votedFor; }
    public void setVotedFor(String candidateId) { this.votedFor = candidateId; }

    public List<LogPayload> getLog() { return log; }

    public long getLastLogIndex() {
        return log.get(log.size() - 1).getIndex();
    }

    public long getLastLogTerm() {
        return log.get(log.size() - 1).getTerm();
    }

    public long getCommitIndex() { return commitIndex; }
    public void setCommitIndex(long commitIndex) { this.commitIndex = commitIndex; }

    public long getLastApplied() { return lastApplied; }
    public void setLastApplied(long lastApplied) { this.lastApplied = lastApplied; }

    public Map<String, Long> getNextIndex() { return nextIndex; }
    public Map<String, Long> getMatchIndex() { return matchIndex; }

    public void initLeaderState(List<String> peerIds) {
        long lastIdx = getLastLogIndex();
        for (String peer : peerIds) {
            nextIndex.put(peer, lastIdx + 1);
            matchIndex.put(peer, 0L);
        }
    }
}
