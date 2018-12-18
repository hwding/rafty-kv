package com.amastigote.raftymicrocluster;

import com.amastigote.raftymicrocluster.protocol.LogEntry;
import com.amastigote.raftymicrocluster.protocol.Role;
import com.amastigote.raftymicrocluster.thread.HeartBeatThread;
import com.amastigote.raftymicrocluster.thread.HeartBeatWatchdogThread;
import com.amastigote.raftymicrocluster.thread.VoteResWatchdogThread;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Objects;

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

    private static int currentTerm = 0;

    /* refer to com.amastigote.raftymicrocluster.protocol.GeneralMsg.responseToPort, voted for candidateId */
    private static int votedFor = 0;

    private static int voteCnt = 0;

    private static RemoteCommunicationParamPack paramPack;

    /* >> LEADER only */
    /* not thread-safe, sync before altering data */
    private static HashMap<Integer, Integer> nextIdx;
    private static HashMap<Integer, Integer> matchIdx;
    /* << LEADER only */

    private static LogEntry[] logs;
    private static int committedIdx;
    private static int appliedIdx;

    synchronized static void init(int nodePort, int totalNodeCnt) {
        NodeStatus.nodePort = nodePort;
        NodeStatus.totalNodeCnt = totalNodeCnt;
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

    public static synchronized void setRoleTo(Role newRole) {
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
        if (votedFor != 0) {
            votedFor = candidateId;

            log.info("voted for {} in current term {}", votedFor, currentTerm);
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
}
