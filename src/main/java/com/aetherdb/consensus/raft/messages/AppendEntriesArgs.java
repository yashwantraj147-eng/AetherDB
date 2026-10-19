package com.aetherdb.consensus.raft.messages;

import com.aetherdb.consensus.raft.LogPayload;

import java.util.Collections;
import java.util.List;

/**
 * Invoked by leader to replicate log entries and serve as periodic heartbeats.
 */
public class AppendEntriesArgs {

    private final long term;
    private final String leaderId;
    private final long prevLogIndex;
    private final long prevLogTerm;
    private final List<LogPayload> entries;
    private final long leaderCommit;

    public AppendEntriesArgs(long term, String leaderId, long prevLogIndex, long prevLogTerm,
                             List<LogPayload> entries, long leaderCommit) {
        this.term = term;
        this.leaderId = leaderId;
        this.prevLogIndex = prevLogIndex;
        this.prevLogTerm = prevLogTerm;
        this.entries = entries != null ? Collections.unmodifiableList(entries) : Collections.emptyList();
        this.leaderCommit = leaderCommit;
    }

    public long getTerm() { return term; }
    public String getLeaderId() { return leaderId; }
    public long getPrevLogIndex() { return prevLogIndex; }
    public long getPrevLogTerm() { return prevLogTerm; }
    public List<LogPayload> getEntries() { return entries; }
    public long getLeaderCommit() { return leaderCommit; }
    public boolean isHeartbeat() { return entries.isEmpty(); }
}
