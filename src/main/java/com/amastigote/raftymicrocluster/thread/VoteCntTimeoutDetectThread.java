package com.amastigote.raftymicrocluster.thread;

import com.amastigote.raftymicrocluster.NodeStatus;
import com.amastigote.raftymicrocluster.procedure.ReElectionInitiateProcedure;
import com.amastigote.raftymicrocluster.protocol.Role;
import com.amastigote.raftymicrocluster.protocol.Timeout;
import lombok.extern.slf4j.Slf4j;

import java.util.Random;

/**
 * @author: hwding
 * @date: 2018/11/29
 */
@SuppressWarnings("JavaDoc")
@Slf4j(topic = "[VOTE CNT TIMEOUT DETC THREAD]")
public class VoteCntTimeoutDetectThread extends Thread {
    private long heartBeatTimeout = new Random(System.nanoTime()).nextInt(Math.toIntExact(Timeout.ELECTION_TIMEOUT_ADDITIONAL_RANGE)) + Timeout.ELECTION_TIMEOUT_BASE;

    @Override
    public void run() {
        try {
            synchronized (this) {
                super.wait(heartBeatTimeout);
                log.info("not collecting enough votes after {} ms", heartBeatTimeout);
                if (isSplitVote()) {
                    new ReElectionInitiateProcedure().start();
                }
            }
        } catch (InterruptedException e) {
            if (isSplitVote()) {
                new ReElectionInitiateProcedure().start();
                log.warn("split vote detected, start a new re-election procedure");
            }
            log.info("is LEADER or has other LEADER, exit without any further action");
        }
    }

    private boolean isSplitVote() {
        return (!isLeaderNow()) && (!hasOtherLeaderNow());
    }

    /* recheck */
    private boolean isLeaderNow() {
        synchronized (NodeStatus.class) {
            if (NodeStatus.voteCnt() >= NodeStatus.majorityNodeCnt()) {
                log.info("nice, i'm leader now with voteCnt {}", NodeStatus.voteCnt());
                synchronized (NodeStatus.class) {
                    NodeStatus.setRoleTo(Role.LEADER);
                    NodeStatus.heartbeatThread().start();
                    NodeStatus.heartbeatRecvTimeoutDetectThread().interrupt();
                }
                return true;
            }
            return false;
        }
    }

    /* check if the candidate step down during the campaign */
    private boolean hasOtherLeaderNow() {
        return Role.FOLLOWER.equals(NodeStatus.role());
    }
}
