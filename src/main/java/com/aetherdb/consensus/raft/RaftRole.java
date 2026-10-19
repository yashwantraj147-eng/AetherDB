package com.aetherdb.consensus.raft;

/**
 * Roles that a node can assume in the Raft consensus state machine.
 */
public enum RaftRole {
    LEADER,
    FOLLOWER,
    CANDIDATE
}
