package com.aetherdb.consensus.raft.messages;

/**
 * Invoked by candidates to gather votes during Raft leader election.
 */
public class RequestVoteArgs {

    private final long term;
    private final String candidateId;
    private final long lastLogIndex;
    private final long lastLogTerm;

    public RequestVoteArgs(long term, String candidateId, long lastLogIndex, long lastLogTerm) {
        this.term = term;
        this.candidateId = candidateId;
        this.lastLogIndex = lastLogIndex;
        this.lastLogTerm = lastLogTerm;
    }

    public long getTerm() { return term; }
    public String getCandidateId() { return candidateId; }
    public long getLastLogIndex() { return lastLogIndex; }
    public long getLastLogTerm() { return lastLogTerm; }
}
