package com.aetherdb.consensus.raft.messages;

/**
 * Reply sent back to leader following an AppendEntries RPC evaluation.
 */
public class AppendEntriesReply {

    private final long term;
    private final boolean success;
    private final long matchIndex;

    public AppendEntriesReply(long term, boolean success, long matchIndex) {
        this.term = term;
        this.success = success;
        this.matchIndex = matchIndex;
    }

    public long getTerm() { return term; }
    public boolean isSuccess() { return success; }
    public long getMatchIndex() { return matchIndex; }
}
