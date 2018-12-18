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

    private static List<LogEntry> entries = new ArrayList<>();

    /* last idx of entries which is replicated to the majority of the cluster (committed)
     * is the criteria of 'is safe to apply?' */
    private static volatile int committedIdx = -1;

    /* last idx of entries which is applied to the current state machine */
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
        if ((!Role.CANDIDATE.equals(NodeStatus.role)) && Role.CANDIDATE.equals(newRole)) {
            log.info("role transfer to {}, reset vote counter", Role.CANDIDATE.toString());
        }

        if (role.equals(newRole)) {
            log.warn("unnecessary role transfer: already {}", role.toString());
            return;
        }

        if (role.equals(Role.FOLLOWER) && newRole.equals(Role.LEADER)) {
            log.error("illegal role transfer: {} -> {}", Role.FOLLOWER.toString(), Role.LEADER.toString());
            System.exit(-1);
        }

        log.warn("role transfer applied: {} -> {}", role.toString(), newRole.toString());
        role = newRole;
    }

    static void setParamPack(RemoteCommunicationParamPack paramPack) {
        NodeStatus.paramPack = paramPack;
    }

    public static RemoteCommunicationParamPack paramPack() {
        return paramPack;
    }

    public static synchronized int incrVoteCnt() {
        return ++voteCnt;
    }

    public static synchronized void rstVoteCnt(int newValue) {
        voteCnt = newValue;
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

    public static int committedIdx() {
        return committedIdx;
    }

    /* LEADER use only */
    private static synchronized void recalculateCommittedIdx() {
        List<Integer> replicatedIdx = new ArrayList<>(followerReplicatedIdxMap.values());
        replicatedIdx.add(entries.size() - 1);

        log.info("we have replicated {} currently in cluster", replicatedIdx);

        replicatedIdx.sort(null);
        int clusterCommittedIdx = replicatedIdx.get(majorityNodeCnt() - 1);

        log.info("we have recalculate the clusterCommittedIdx as {}", clusterCommittedIdx);
        committedIdx = clusterCommittedIdx;

        applyEntry(committedIdx);
    }

    public static int appliedIdx() {
        return appliedIdx;
    }

    /* LEADER use only */
    public synchronized static void updateFollowerEntriesState(
            final int followerPort,
            final int lastReplicatedLogIdx
    ) {
        final int oldLastReplicatedLogIdx = followerReplicatedIdxMap.getOrDefault(followerPort, -1);

        if (oldLastReplicatedLogIdx > lastReplicatedLogIdx) {
            log.warn("violet idx constraint: oldLastReplicatedLogIdx {} > lastReplicatedLogIdx {}, ignore", oldLastReplicatedLogIdx, lastReplicatedLogIdx);
        } else if (oldLastReplicatedLogIdx < lastReplicatedLogIdx) {
            log.info("followerReplicatedIdxMap updated for {}: {} -> {}", followerPort, oldLastReplicatedLogIdx, lastReplicatedLogIdx);
            followerReplicatedIdxMap.put(followerPort, lastReplicatedLogIdx);
        }

        recalculateCommittedIdx();
    }

    public synchronized static FollowerAppendEntryResultContext appendEntry(
            final List<LogEntry> residualLogs,
            final int prevLogIdx,
            final int prevLogTerm,
            final int appliedIdx,
            final int committedIdx
    ) {
        FollowerAppendEntryResultContext context = new FollowerAppendEntryResultContext();

        if (Objects.isNull(residualLogs) || residualLogs.isEmpty()) {
            log.info("simple heartbeat without entries, no appending is needed");
            context.setNeedRespond(false);
            return context;
        }

        if (Role.FOLLOWER.equals(role)) {
            /* simply update the global known committedIdx */
            NodeStatus.committedIdx = NodeStatus.committedIdx < committedIdx ? committedIdx : NodeStatus.committedIdx;
        }

        int currentLastIdx = entries.size() - 1;

        if (currentLastIdx < prevLogIdx) {
            log.error("currentLastIdx {} < prevLogIdx {}, may create a hole in entries, ignore", currentLastIdx, prevLogIdx);

            context.setNeedRespond(false);
        } else {
            int currentPrevLogTerm = entries.get(prevLogIdx).getTerm();
            if (currentPrevLogTerm != prevLogTerm) {
                log.warn("we have a log consistent issue, remove old log at {}", prevLogIdx);

                /* do nothing but truncate current entries from the prev entry */
                entries = new ArrayList<>(entries.subList(0, prevLogIdx));

                context.setNeedRespond(true);
            } else if (currentLastIdx >= prevLogIdx + residualLogs.size()) {
                log.warn("current entries covers the residual entries, ignore");

                context.setNeedRespond(false);
            } else {
                /* truncate residual entries to currentLastIdx */
                int truncateOffset = currentLastIdx > prevLogIdx ? currentLastIdx - prevLogIdx : 0;

                entries.addAll(residualLogs.subList(truncateOffset, residualLogs.size()));
                applyEntry(appliedIdx);

                log.info("entries updated: {}", entries);

                context.setNeedRespond(true);
            }
        }

        /* setup necessary info */
        context.setLastReplicatedLogIdx(entries.size());

        return context;
    }

    private synchronized static void applyEntry(final int appliedToIdx) {
        if (appliedToIdx >= entries.size()) {
            log.error("we have a illegal apply issue, appliedToIdx {} >= entries.size() {}, give up", appliedToIdx, entries.size());
            return;
        }

        appliedIdx = appliedToIdx;
    }

    public static FollowerResidualEntryInfo genResidualEntryInfoForFollower(final int followerPort) {
        final int oldLastReplicatedLogIdx = followerReplicatedIdxMap.getOrDefault(followerPort, -1);
        if (oldLastReplicatedLogIdx >= entries.size()) {
            log.error("invalid state detected, oldLastReplicatedLogIdx {} >= entries.size() {}, try recovering by resetting related val", oldLastReplicatedLogIdx, entries.size());
            followerReplicatedIdxMap.put(followerPort, -1);
        }

        FollowerResidualEntryInfo.FollowerResidualEntryInfoBuilder builder = FollowerResidualEntryInfo.builder();
        if (oldLastReplicatedLogIdx < entries.size() - 1) {

            /* setup necessary info */
            if (oldLastReplicatedLogIdx >= 0) {
                builder
                        .prevLogIdx(oldLastReplicatedLogIdx)
                        .prevLogTerm(entries.get(oldLastReplicatedLogIdx).getTerm());
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
