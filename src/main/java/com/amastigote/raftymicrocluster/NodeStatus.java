package com.amastigote.raftymicrocluster;

import com.amastigote.raftymicrocluster.protocol.LogEntry;
import com.amastigote.raftymicrocluster.protocol.Role;
import com.amastigote.raftymicrocluster.thread.HeartBeatThread;
import com.amastigote.raftymicrocluster.thread.HeartBeatWatchdogThread;
import com.amastigote.raftymicrocluster.thread.VoteResWatchdogThread;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author: hwding
 * @date: 2018/11/28
 */
@SuppressWarnings("JavaDoc")
@Slf4j(topic = "[NODE STATUS]")
public final class NodeStatus {

    private static int nodePort;
    private static Role role = Role.FOLLOWER;

    private static int totalNodeCnt;

    private static Thread heartbeatThread;
    private static Thread voteResWatchdogThread;
    private static Thread heartBeatWatchdogThread;

    private static Storage storage = new Storage();

    private static volatile int currentTerm = 0;

    /* refer to com.amastigote.raftymicrocluster.protocol.GeneralMsg.responseToPort, voted for candidateId */
    private static volatile int votedFor = -1;

    private static volatile int voteCnt = 0;

    private static RemoteCommunicationParamPack paramPack;

    /* >> LEADER only
     * CAUTION: non-thread-safe, sync before altering data */
    /* highest idx of entry which replicated to follower */
    private static Map<Integer, Integer> followerReplicatedIdxMap;
    /* << LEADER only */

    private static List<LogEntry> entries = new ArrayList<LogEntry>() {{
        add(new LogEntry("__INIT", null, LogEntry.LogCommandType.REMOVE));
    }};

    /* global(remote): last idx of entries which is replicated to the majority of the cluster (committed)
     * is the criteria of 'is it safe to apply?' */
    private static volatile int leaderCommittedIdx = -1;

    /* local: last idx of entries which is applied to the current state machine */
    private static volatile int appliedIdx = -1;

    synchronized static void init(int nodePort, int totalNodeCnt) {
        NodeStatus.nodePort = nodePort;
        NodeStatus.totalNodeCnt = totalNodeCnt;
        NodeStatus.followerReplicatedIdxMap = new HashMap<>(totalNodeCnt - 1);
    }

    public static int majorityNodeCnt() {
        return totalNodeCnt / 2 + 1;
    }

    public static int nodePort() {
        return nodePort;
    }

    public static int currentTerm() {
        return currentTerm;
    }

    public static synchronized int incrCurrentTerm() {
        return ++currentTerm;
    }

    public static synchronized void updateTerm(int newTerm) {
        if (currentTerm >= newTerm) {
            log.warn("bad term transfer: {} -> {}, ignore", currentTerm, newTerm);
            return;
        }

        currentTerm = newTerm;
    }

    public static synchronized void rstVoteResWatchdogThread(boolean fire) {
        if (Objects.nonNull(voteResWatchdogThread) && voteResWatchdogThread.isAlive()) {
            voteResWatchdogThread.interrupt();
        }

        voteResWatchdogThread = new VoteResWatchdogThread();

        if (fire) {
            voteResWatchdogThread.start();
        }
    }

    public static synchronized void rstHeartbeatThread(boolean fire) {
        if (Objects.nonNull(heartbeatThread) && heartbeatThread.isAlive()) {
            heartbeatThread.interrupt();
        }

        heartbeatThread = new HeartBeatThread();

        if (fire) {
            heartbeatThread.start();
        }
    }

    public static synchronized void rstHeartBeatWatchdogThread(boolean fire) {
        if (Objects.nonNull(heartBeatWatchdogThread) && heartBeatWatchdogThread.isAlive()) {
            heartBeatWatchdogThread.interrupt();
        }

        heartBeatWatchdogThread = new HeartBeatWatchdogThread();

        if (fire) {
            heartBeatWatchdogThread.start();
        }
    }

    public static synchronized void transferRoleTo(Role newRole) {
        if (role.equals(newRole)) {
            log.warn("unnecessary role transfer: already {}", role.toString());
            return;
        }

        if (role.equals(Role.FOLLOWER) && newRole.equals(Role.LEADER)) {
            log.error("illegal role transfer: {} -> {}", Role.FOLLOWER.toString(), Role.LEADER.toString());
            System.exit(-1);
        }

        if ((!Role.CANDIDATE.equals(NodeStatus.role)) && Role.CANDIDATE.equals(newRole)) {
            log.info("role transfer to {}, reset vote counter", newRole);
            rstVoteCnt();
        }

        if ((!Role.LEADER.equals(NodeStatus.role)) && Role.LEADER.equals(newRole)) {
            log.info("role transfer to {}, reset followerReplicatedIdxMap to {}", newRole, entries.size());
            initFollowerReplicatedIdxMap(entries.size() - 1);
        }

        log.warn("role transfer applied: {} -> {}", role.toString(), newRole.toString());
        role = newRole;
    }

    static void initParamPack(RemoteCommunicationParamPack paramPack) {
        NodeStatus.paramPack = paramPack;
    }

    static void initFollowerReplicatedIdxMap() {
        initFollowerReplicatedIdxMap(-1);
    }

    private static void initFollowerReplicatedIdxMap(int newVal) {
        if (Objects.isNull(NodeStatus.paramPack)) {
            log.error("init followerReplicatedIdxMap after init paramPack");
            System.exit(-1);
        }

        paramPack.getCommunicationTargets().forEach(e ->
                followerReplicatedIdxMap.put(e.getPort(), newVal)
        );
    }

    public static RemoteCommunicationParamPack paramPack() {
        return paramPack;
    }

    public static synchronized int incrVoteCnt() {
        return ++voteCnt;
    }

    private static synchronized void rstVoteCnt() {
        voteCnt = 1;
    }

    public static synchronized void rstVotedFor() {
        votedFor = 0;
    }

    public static int votedFor() {
        return votedFor;
    }

    public static Role role() {
        return role;
    }

    public static Storage storage() {
        return storage;
    }

    public static synchronized void voteFor(int candidateId) {
        if (votedFor == 0) {
            votedFor = candidateId;

            log.info("voted for {} in current term {}", votedFor, currentTerm);
            return;
        }

        log.warn("already voted for {} in current term {}, give up", votedFor, currentTerm);
    }

    public static Thread heartBeatWatchdogThread() {
        return heartBeatWatchdogThread;
    }

    public static Thread voteResWatchdogThread() {
        return voteResWatchdogThread;
    }

    public static Thread heartbeatThread() {
        return heartbeatThread;
    }

    public static int leaderCommittedIdx() {
        return leaderCommittedIdx;
    }

    /* LEADER use only */
    private static synchronized void recalculateCommittedIdx() {
        List<Integer> replicatedIdx = new ArrayList<>(followerReplicatedIdxMap.values());
        replicatedIdx.add(entries.size() - 1);

        log.info("we have replicated idx {} currently in cluster", replicatedIdx);

        replicatedIdx.sort(null);
        int clusterCommittedIdx = replicatedIdx.get(majorityNodeCnt() - 1);

        log.info("we have recalculate the clusterCommittedIdx as {}", clusterCommittedIdx);
        leaderCommittedIdx = clusterCommittedIdx;

        applyEntry();
    }

    /* LEADER use only */
    public synchronized static void updateFollowerEntriesState(
            final int followerPort,
            final int lastReplicatedLogIdx
    ) {
        if (!Role.LEADER.equals(role)) {
            log.error("violet role check in updateFollowerEntriesState, ignore");
            return;
        }

        final int oldLastReplicatedLogIdx = followerReplicatedIdxMap.getOrDefault(followerPort, -1);

        if (oldLastReplicatedLogIdx > lastReplicatedLogIdx) {
            log.warn("follower {}'s lastReplicatedLogIdx may need fix from {} to {} due to inconsistency", followerPort, oldLastReplicatedLogIdx, lastReplicatedLogIdx);
        } else if (oldLastReplicatedLogIdx < lastReplicatedLogIdx) {
            log.info("followerReplicatedIdxMap updated for {}: {} -> {}", followerPort, oldLastReplicatedLogIdx, lastReplicatedLogIdx);
        }
        followerReplicatedIdxMap.put(followerPort, lastReplicatedLogIdx);

        recalculateCommittedIdx();
    }

    /* LEADER use only */
    public synchronized static void appendEntryFromClient(List<LogEntry> entries) {
        if (!Role.LEADER.equals(role)) {
            log.error("violet role check in appendEntryFromClient, ignore");
            return;
        }

        entries = entries.stream()
                .peek(e -> e.setTerm(currentTerm))
                .collect(Collectors.toCollection(ArrayList::new));
        NodeStatus.entries.addAll(entries);
    }

    /* FOLLOWER use only */
    public synchronized static FollowerAppendEntryResultContext appendEntry(
            final List<LogEntry> residualLogs,
            final int prevLogIdx,
            final int prevLogTerm,
            final int leaderCommittedIdx
    ) {
        FollowerAppendEntryResultContext context = new FollowerAppendEntryResultContext();

        if (!Role.FOLLOWER.equals(role)) {
            log.error("violet role check in appendEntry, ignore");

            context.setNeedRespond(false);
            return context;
        }

        if (prevLogIdx > entries.size() - 1) {
            log.warn("seems that we lost some of the entries, report issue");

            context.setLastReplicatedLogIdx(entries.size() - 1);
            context.setNeedRespond(true);
            return context;
        }

        if ((Objects.isNull(residualLogs) || residualLogs.isEmpty()) && appliedIdx == leaderCommittedIdx) {
            log.info("simple heartbeat without entries and new apply idx, do nothing");

            context.setNeedRespond(false);
            return context;
        }

        if ((Objects.isNull(residualLogs) || residualLogs.isEmpty()) && appliedIdx != leaderCommittedIdx) {
            log.info("no entries to append but new leader apply idx detected");

            applyEntry(leaderCommittedIdx);

            context.setNeedRespond(false);
            return context;
        }

        /* >> append log */
        boolean safeToApply = false;
        int currentLastIdx = entries.size() - 1;
        if (currentLastIdx < prevLogIdx) {
            log.error("currentLastIdx {} < prevLogIdx {}, may create a hole in entries, ignore appending but inform leader", currentLastIdx, prevLogIdx);

            context.setNeedRespond(true);
        } else {
            if (prevLogIdx != -1 && entries.get(prevLogIdx).getTerm() != prevLogTerm) {
                log.warn("we have a log consistent issue, remove old log at {}", prevLogIdx);

                /* do nothing but truncate current entries from the prev entry */
                entries = new ArrayList<>(entries.subList(0, prevLogIdx));

                context.setNeedRespond(true);
            } else if (currentLastIdx >= prevLogIdx + residualLogs.size()) {
                log.warn("current entries covers the residual entries, ignore appending but inform leader");

                safeToApply = true;
                context.setNeedRespond(true);
            } else {
                /* truncate residual entries to currentLastIdx */
                int truncateOffset = currentLastIdx > prevLogIdx ? currentLastIdx - prevLogIdx : 0;

                entries.addAll(residualLogs.subList(truncateOffset, residualLogs.size()));

                log.info("entries updated: {}", entries);

                safeToApply = true;
                context.setNeedRespond(true);
            }
        }
        /* << append log */

        /* only do apply when there's no consistency issue */
        if (safeToApply) {
            applyEntry(leaderCommittedIdx);
        }

        /* setup necessary info */
        context.setLastReplicatedLogIdx(entries.size() - 1);

        return context;
    }

    /* FOLLOWER use only, wrapper for applyEntry() */
    private synchronized static void applyEntry(int newLeaderCommittedIdx) {
        if (!Role.FOLLOWER.equals(role)) {
            log.error("violet role check in applyEntry, ignore");
            return;
        }

        /* simply update the leaderCommittedIdx */
        if (leaderCommittedIdx > newLeaderCommittedIdx) {
            log.error("we have failed a constraint check which leaderCommittedIdx {} > newLeaderCommittedIdx {}, try recovering by resetting related val", leaderCommittedIdx, newLeaderCommittedIdx);
        }
        leaderCommittedIdx = newLeaderCommittedIdx;

        applyEntry();
    }

    /* LEADER direct use only, but do not do role check */
    private synchronized static void applyEntry() {
        if (leaderCommittedIdx >= entries.size()) {
            log.error("we have an illegal apply issue, leaderCommittedIdx {} >= entries.size() {}, give up", leaderCommittedIdx, entries.size());
            return;
        }

        if (leaderCommittedIdx <= appliedIdx) {
            log.info("already applied to {} which is at least as large as {}, ignore", appliedIdx, leaderCommittedIdx);
            return;
        }

        /* >> do apply to storage */
        List<LogEntry> entriesToApply = entries.subList(appliedIdx + 1, leaderCommittedIdx + 1);
        storage.applyEntryCommands(entriesToApply);
        /* << do apply to storage */

        appliedIdx = leaderCommittedIdx;
        log.info("local entries applied to {}", appliedIdx);
    }

    public static FollowerResidualEntryInfo genResidualEntryInfoForFollower(final int followerPort) {
        final int oldLastReplicatedLogIdx = followerReplicatedIdxMap.getOrDefault(followerPort, -1);
        if (oldLastReplicatedLogIdx >= entries.size()) {
            log.error("invalid state detected, oldLastReplicatedLogIdx {} >= entries.size() {}, try recovering by resetting related val", oldLastReplicatedLogIdx, entries.size());
            followerReplicatedIdxMap.put(followerPort, -1);
        }

        FollowerResidualEntryInfo.FollowerResidualEntryInfoBuilder builder = FollowerResidualEntryInfo.builder();
        builder.prevLogIdx(oldLastReplicatedLogIdx);

        if (oldLastReplicatedLogIdx < entries.size() - 1) {

            /* setup necessary info */
            if (oldLastReplicatedLogIdx >= 0) {
                builder.prevLogTerm(entries.get(oldLastReplicatedLogIdx).getTerm());
            }
        } else {
            log.info("follower {} already up-to-date as idx {}", followerPort, oldLastReplicatedLogIdx);
        }

        return builder.residualLogs(new ArrayList<>(entries.subList(oldLastReplicatedLogIdx + 1, entries.size()))).build();
    }

    /* LEADER use only */
    @Builder
    @Getter
    public static final class FollowerResidualEntryInfo {

        @Builder.Default
        private List<LogEntry> residualLogs = null;

        @Builder.Default
        private int prevLogIdx = -1;

        @Builder.Default
        private int prevLogTerm = -1;
    }

    /* FOLLOWER use only */
    @Setter
    @Getter
    @ToString
    public static class FollowerAppendEntryResultContext {
        private boolean needRespond = true;
        private int lastReplicatedLogIdx = -1;
        private int resToPort = -1;
    }
}
