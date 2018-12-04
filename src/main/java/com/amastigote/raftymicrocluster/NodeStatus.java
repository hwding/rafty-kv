package com.amastigote.raftymicrocluster;

import com.amastigote.raftymicrocluster.protocol.Role;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author: hwding
 * @date: 2018/11/28
 */
@SuppressWarnings("JavaDoc")
@Slf4j(topic = "[NODE STATUS]")
public final class NodeStatus {

    private static String nodeName;
    private static int nodePort;
    private static Role role = Role.FOLLOWER;
    private static AtomicInteger voteCnt = new AtomicInteger();

    private static Thread heartbeatThread;
    private static Thread voteCntTimeoutDetectThread;
    private static Thread heartbeatRecvTimeoutDetectThread;

    private static AtomicInteger currentTerm = new AtomicInteger(0);
//    private static AtomicInteger votedTerm = new AtomicInteger(-1);

    /* refer to com.amastigote.raftymicrocluster.protocol.GeneralMsg.responseToPort, voted for candidateId */
    private static AtomicInteger votedFor = new AtomicInteger(0);

    private static int totalNodeCnt;

    private static CommunicateToOthersParamPack paramPack;

    synchronized static void init(String nodeName, int nodePort, int totalNodeCnt) {
        NodeStatus.nodeName = nodeName;
        NodeStatus.nodePort = nodePort;
        NodeStatus.totalNodeCnt = totalNodeCnt;
    }

    public static int majorityNodeCnt() {
        return totalNodeCnt / 2 + 1;
    }

    public static String nodeName() {
        return nodeName;
    }

    public static int nodePort() {
        return NodeStatus.nodePort;
    }

    public static int incrTerm() {
        return currentTerm.incrementAndGet();
    }

    public static int currentTerm() {
        return currentTerm.get();
    }

    public static void reInitTerm(int term) {
        currentTerm.set(term);
    }

    public static Thread voteCntTimeoutDetectThread() {
        return voteCntTimeoutDetectThread;
    }

    public static void setVoteCntTimeoutDetectThread(Thread voteCntTimeoutDetectThread) {
        NodeStatus.voteCntTimeoutDetectThread = voteCntTimeoutDetectThread;
    }

    public static synchronized void setRoleTo(Role newRole) {
        if ((!Role.CANDIDATE.equals(NodeStatus.role)) && Role.CANDIDATE.equals(newRole)) {
            log.info("role transfer to {}, reset vote counter", Role.CANDIDATE.toString());
        }

        if (role.equals(newRole)) {
            log.warn("unnecessary role transfer: already {}", role.toString());
        }

        if (role.equals(Role.FOLLOWER) && newRole.equals(Role.LEADER)) {
            log.error("illegal role transfer: {} -> {}", Role.FOLLOWER.toString(), Role.LEADER.toString());
            System.exit(-1);
        }

        log.warn("role transfer applied: {} -> {}", role.toString(), newRole.toString());
        role = newRole;
    }

    static void setParamPack(CommunicateToOthersParamPack paramPack) {
        NodeStatus.paramPack = paramPack;
    }

    public static CommunicateToOthersParamPack paramPack() {
        return NodeStatus.paramPack;
    }

    public static int incrVoteCnt() {
        return voteCnt.incrementAndGet();
    }

    public static void resetVoteCnt(int newValue) {
        voteCnt.set(newValue);
    }

    public static void resetVotedFor() {
        NodeStatus.votedFor.set(0);
    }

    public static int voteCnt() {
        return voteCnt.get();
    }

    public static int votedFor() {
        return NodeStatus.votedFor.get();
    }

//    public static Integer votedTerm() {
//        return NodeStatus.votedTerm.get();
//    }

    public static Role role() {
        return NodeStatus.role;
    }

//    public static void updateVotedTerm(int term) {
//        while (true) {
//            int oldVotedTerm = NodeStatus.votedTerm.get();
//            if (oldVotedTerm >= term) {
//                log.warn("illegal voted term transfer: votedTerm {} >= current vote term {}", oldVotedTerm, term);
//                break;
//            }
//
//            boolean set = NodeStatus.votedTerm.compareAndSet(oldVotedTerm, term);
//
//            if (set) {
//                break;
//            }
//        }
//    }

    public static boolean voteFor(Integer candidateId) {
        boolean set = false;
        while (!set) {
            int votedFor = NodeStatus.votedFor.get();

            if (votedFor != 0) {
                log.warn("already voted for {} in current term, give up", votedFor);
                return false;
            }

            set = NodeStatus.votedFor.compareAndSet(0, candidateId);
        }

        return true;
    }

    static void setHeartbeatThread(Thread thread) {
        NodeStatus.heartbeatThread = thread;
    }

    public static void setHeartbeatRecvTimeoutDetectThread(Thread thread) {
        NodeStatus.heartbeatRecvTimeoutDetectThread = thread;
    }

    public static Thread heartbeatRecvTimeoutDetectThread() {
        return NodeStatus.heartbeatRecvTimeoutDetectThread;
    }

    public static Thread heartbeatThread() {
        return NodeStatus.heartbeatThread;
    }
}
