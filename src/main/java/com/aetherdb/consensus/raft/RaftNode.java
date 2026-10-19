package com.aetherdb.consensus.raft;

import com.aetherdb.common.AetherConfig;
import com.aetherdb.consensus.raft.messages.*;
import com.aetherdb.storage.engine.StorageEngine;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Full Raft Consensus state machine implementing Leader Election,
 * Log Replication, Heartbeat broadcasting, and State Machine Application.
 */
public class RaftNode implements AutoCloseable {

    private final String nodeId;
    private final AetherConfig config;
    private final RaftState state;
    private final StorageEngine storageEngine;
    private final Map<String, PeerClient> peers = new ConcurrentHashMap<>();

    private volatile RaftRole currentRole = RaftRole.FOLLOWER;
    private volatile String currentLeaderId = null;
    private volatile long lastHeartbeatTimestamp = System.currentTimeMillis();

    private final ScheduledExecutorService timerExecutor;
    private final Random random = new Random();
    private volatile boolean running = true;

    public RaftNode(AetherConfig config, StorageEngine storageEngine) {
        this.nodeId = config.getNodeId();
        this.config = config;
        this.storageEngine = storageEngine;
        this.state = new RaftState();

        for (AetherConfig.PeerNode peer : config.getClusterPeers()) {
            if (!peer.getId().equalsIgnoreCase(nodeId)) {
                peers.put(peer.getId(), new PeerClient(peer));
            }
        }

        this.timerExecutor = Executors.newScheduledThreadPool(2,
                Thread.ofVirtual().name("raft-timer-", 1).factory()
        );

        // Single node cluster automatically becomes leader
        if (peers.isEmpty()) {
            currentRole = RaftRole.LEADER;
            currentLeaderId = nodeId;
        } else {
            // Schedule randomized election timer check
            long checkInterval = config.getRaftElectionMinMs() / 2;
            timerExecutor.scheduleWithFixedDelay(this::checkElectionTimeout,
                    checkInterval, checkInterval, TimeUnit.MILLISECONDS);

            // Schedule heartbeat dispatcher for when leader
            timerExecutor.scheduleWithFixedDelay(this::broadcastHeartbeatsIfLeader,
                    config.getRaftHeartbeatMs(), config.getRaftHeartbeatMs(), TimeUnit.MILLISECONDS);
        }
    }

    public String getNodeId() { return nodeId; }
    public RaftRole getCurrentRole() { return currentRole; }
    public String getCurrentLeaderId() { return currentLeaderId; }
    public long getCurrentTerm() { return state.getCurrentTerm(); }
    public RaftState getState() { return state; }
    public boolean isLeader() { return currentRole == RaftRole.LEADER; }

    /**
     * Periodic check to determine if an election should be started.
     */
    private synchronized void checkElectionTimeout() {
        if (!running || currentRole == RaftRole.LEADER) return;

        long elapsed = System.currentTimeMillis() - lastHeartbeatTimestamp;
        long timeout = config.getRaftElectionMinMs() + random.nextInt(config.getRaftElectionMaxMs() - config.getRaftElectionMinMs());

        if (elapsed >= timeout) {
            startElection();
        }
    }

    /**
     * Initiates a new election campaign.
     */
    private synchronized void startElection() {
        currentRole = RaftRole.CANDIDATE;
        state.setCurrentTerm(state.getCurrentTerm() + 1);
        state.setVotedFor(nodeId);
        lastHeartbeatTimestamp = System.currentTimeMillis();

        long term = state.getCurrentTerm();
        long lastLogIndex = state.getLastLogIndex();
        long lastLogTerm = state.getLastLogTerm();

        int totalNodes = peers.size() + 1;
        int quorum = (totalNodes / 2) + 1;
        AtomicInteger votes = new AtomicInteger(1); // Self vote

        for (PeerClient peer : peers.values()) {
            RequestVoteArgs args = new RequestVoteArgs(term, nodeId, lastLogIndex, lastLogTerm);
            peer.requestVote(args).thenAccept(reply -> {
                synchronized (this) {
                    if (reply.getTerm() > state.getCurrentTerm()) {
                        stepDownToFollower(reply.getTerm());
                        return;
                    }
                    if (currentRole == RaftRole.CANDIDATE && reply.getTerm() == term && reply.isVoteGranted()) {
                        if (votes.incrementAndGet() >= quorum) {
                            becomeLeader();
                        }
                    }
                }
            });
        }
    }

    private synchronized void becomeLeader() {
        if (currentRole != RaftRole.CANDIDATE) return;
        currentRole = RaftRole.LEADER;
        currentLeaderId = nodeId;
        state.initLeaderState(new ArrayList<>(peers.keySet()));
        broadcastHeartbeats();
    }

    private synchronized void stepDownToFollower(long newTerm) {
        currentRole = RaftRole.FOLLOWER;
        state.setCurrentTerm(newTerm);
        state.setVotedFor(null);
        lastHeartbeatTimestamp = System.currentTimeMillis();
    }

    /**
     * Broadcasts heartbeats to all cluster peers if this node is leader.
     */
    private void broadcastHeartbeatsIfLeader() {
        if (currentRole == RaftRole.LEADER) {
            broadcastHeartbeats();
        }
    }

    private synchronized void broadcastHeartbeats() {
        long term = state.getCurrentTerm();
        long commitIndex = state.getCommitIndex();
        long lastLogIndex = state.getLastLogIndex();
        long lastLogTerm = state.getLastLogTerm();

        for (PeerClient peer : peers.values()) {
            AppendEntriesArgs args = new AppendEntriesArgs(term, nodeId, lastLogIndex, lastLogTerm, Collections.emptyList(), commitIndex);
            peer.appendEntries(args).thenAccept(reply -> {
                synchronized (this) {
                    if (reply.getTerm() > state.getCurrentTerm()) {
                        stepDownToFollower(reply.getTerm());
                    }
                }
            });
        }
    }

    /**
     * Handles an incoming RequestVote RPC from a candidate.
     */
    public synchronized RequestVoteReply handleRequestVote(RequestVoteArgs args) {
        if (args.getTerm() > state.getCurrentTerm()) {
            stepDownToFollower(args.getTerm());
        }

        boolean canVote = (state.getVotedFor() == null || state.getVotedFor().equalsIgnoreCase(args.getCandidateId()));
        boolean termOk = args.getTerm() == state.getCurrentTerm();

        // Check if candidate's log is at least as up-to-date as receiver's log
        boolean logOk = args.getLastLogTerm() > state.getLastLogTerm()
                || (args.getLastLogTerm() == state.getLastLogTerm() && args.getLastLogIndex() >= state.getLastLogIndex());

        if (termOk && canVote && logOk) {
            state.setVotedFor(args.getCandidateId());
            lastHeartbeatTimestamp = System.currentTimeMillis();
            return new RequestVoteReply(state.getCurrentTerm(), true);
        }

        return new RequestVoteReply(state.getCurrentTerm(), false);
    }

    /**
     * Handles an incoming AppendEntries RPC (heartbeat or log replication) from leader.
     */
    public synchronized AppendEntriesReply handleAppendEntries(AppendEntriesArgs args) {
        if (args.getTerm() < state.getCurrentTerm()) {
            return new AppendEntriesReply(state.getCurrentTerm(), false, state.getLastLogIndex());
        }

        if (args.getTerm() > state.getCurrentTerm() || currentRole == RaftRole.CANDIDATE) {
            stepDownToFollower(args.getTerm());
        }

        currentLeaderId = args.getLeaderId();
        lastHeartbeatTimestamp = System.currentTimeMillis();

        // If entries are attached, append them to local state log
        if (!args.getEntries().isEmpty()) {
            for (LogPayload entry : args.getEntries()) {
                state.getLog().add(entry);
            }
        }

        // Apply committed entries to local storage engine
        if (args.getLeaderCommit() > state.getCommitIndex()) {
            long newCommit = Math.min(args.getLeaderCommit(), state.getLastLogIndex());
            state.setCommitIndex(newCommit);
            applyLogEntriesToStorage();
        }

        return new AppendEntriesReply(state.getCurrentTerm(), true, state.getLastLogIndex());
    }

    private void applyLogEntriesToStorage() {
        while (state.getLastApplied() < state.getCommitIndex()) {
            long nextIdx = state.getLastApplied() + 1;
            if (nextIdx < state.getLog().size()) {
                LogPayload payload = state.getLog().get((int) nextIdx);
                try {
                    if (payload.getType() != null) {
                        switch (payload.getType()) {
                            case PUT -> storageEngine.put(payload.getKey(), payload.getValue());
                            case DELETE -> storageEngine.delete(payload.getKey());
                        }
                    }
                } catch (IOException e) {
                    System.err.println("Error applying Raft entry to storage: " + e.getMessage());
                }
                state.setLastApplied(nextIdx);
            } else {
                break;
            }
        }
    }

    @Override
    public void close() {
        running = false;
        timerExecutor.shutdownNow();
    }
}
