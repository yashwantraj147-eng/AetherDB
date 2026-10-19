package com.aetherdb.consensus.raft;

import com.aetherdb.common.AetherConfig;
import com.aetherdb.consensus.raft.messages.AppendEntriesArgs;
import com.aetherdb.consensus.raft.messages.AppendEntriesReply;
import com.aetherdb.consensus.raft.messages.RequestVoteArgs;
import com.aetherdb.consensus.raft.messages.RequestVoteReply;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Asynchronous HTTP/JSON client for inter-node Raft consensus RPC communication.
 * Powered by java.net.http.HttpClient with short connection timeouts.
 */
public class PeerClient {

    private final AetherConfig.PeerNode peer;
    private final HttpClient httpClient;
    private final String baseUrl;

    public PeerClient(AetherConfig.PeerNode peer) {
        this.peer = peer;
        this.baseUrl = "http://" + peer.getHost() + ":" + peer.getPort();
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofMillis(200))
                .build();
    }

    public String getPeerId() {
        return peer.getId();
    }

    /**
     * Sends an asynchronous RequestVote RPC to this peer.
     */
    public CompletableFuture<RequestVoteReply> requestVote(RequestVoteArgs args) {
        String json = String.format("{\"term\":%d,\"candidateId\":\"%s\",\"lastLogIndex\":%d,\"lastLogTerm\":%d}",
                args.getTerm(), args.getCandidateId(), args.getLastLogIndex(), args.getLastLogTerm());

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/api/v1/raft/requestVote"))
                .timeout(Duration.ofMillis(300))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(json))
                .build();

        return httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .thenApply(response -> {
                    if (response.statusCode() == 200) {
                        return parseRequestVoteReply(response.body());
                    }
                    return new RequestVoteReply(args.getTerm(), false);
                })
                .exceptionally(ex -> new RequestVoteReply(args.getTerm(), false));
    }

    /**
     * Sends an asynchronous AppendEntries RPC (heartbeat or log replication) to this peer.
     */
    public CompletableFuture<AppendEntriesReply> appendEntries(AppendEntriesArgs args) {
        String json = String.format("{\"term\":%d,\"leaderId\":\"%s\",\"prevLogIndex\":%d,\"prevLogTerm\":%d,\"leaderCommit\":%d,\"entryCount\":%d}",
                args.getTerm(), args.getLeaderId(), args.getPrevLogIndex(), args.getPrevLogTerm(), args.getLeaderCommit(), args.getEntries().size());

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/api/v1/raft/appendEntries"))
                .timeout(Duration.ofMillis(300))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(json))
                .build();

        return httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .thenApply(response -> {
                    if (response.statusCode() == 200) {
                        return parseAppendEntriesReply(response.body());
                    }
                    return new AppendEntriesReply(args.getTerm(), false, 0);
                })
                .exceptionally(ex -> new AppendEntriesReply(args.getTerm(), false, 0));
    }

    private RequestVoteReply parseRequestVoteReply(String body) {
        long term = 0;
        boolean voteGranted = false;
        for (String part : body.replace("{", "").replace("}", "").split(",")) {
            String[] kv = part.split(":");
            if (kv.length == 2) {
                String k = kv[0].replace("\"", "").trim();
                String v = kv[1].replace("\"", "").trim();
                if ("term".equalsIgnoreCase(k)) term = Long.parseLong(v);
                if ("voteGranted".equalsIgnoreCase(k)) voteGranted = Boolean.parseBoolean(v);
            }
        }
        return new RequestVoteReply(term, voteGranted);
    }

    private AppendEntriesReply parseAppendEntriesReply(String body) {
        long term = 0;
        boolean success = false;
        long matchIndex = 0;
        for (String part : body.replace("{", "").replace("}", "").split(",")) {
            String[] kv = part.split(":");
            if (kv.length == 2) {
                String k = kv[0].replace("\"", "").trim();
                String v = kv[1].replace("\"", "").trim();
                if ("term".equalsIgnoreCase(k)) term = Long.parseLong(v);
                if ("success".equalsIgnoreCase(k)) success = Boolean.parseBoolean(v);
                if ("matchIndex".equalsIgnoreCase(k)) matchIndex = Long.parseLong(v);
            }
        }
        return new AppendEntriesReply(term, success, matchIndex);
    }
}
