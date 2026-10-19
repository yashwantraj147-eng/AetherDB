package com.aetherdb.consensus.raft.messages;

/**
 * Reply sent back to a candidate after a RequestVote RPC evaluation.
 */
public class RequestVoteReply {

    private final long term;
    private final boolean voteGranted;

    public RequestVoteReply(long term, boolean voteGranted) {
        this.term = term;
        this.voteGranted = voteGranted;
    }

    public long getTerm() { return term; }
    public boolean isVoteGranted() { return voteGranted; }
}
