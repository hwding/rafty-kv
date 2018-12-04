package com.amastigote.raftymicrocluster.thread;

import com.amastigote.raftymicrocluster.NodeStatus;
import com.amastigote.raftymicrocluster.procedure.ReElectionInitiateProcedure;
import com.amastigote.raftymicrocluster.protocol.Role;
import com.amastigote.raftymicrocluster.protocol.TimeSpan;
import lombok.extern.slf4j.Slf4j;

import java.util.Random;

/**
 * @author: hwding
 * @date: 2018/11/29
 */
@SuppressWarnings("JavaDoc")
@Slf4j(topic = "[VOTE CNT TIMEOUT DETC THREAD]")
public class VoteCntTimeoutDetectThread extends Thread {
    private final static long electionTimeout = new Random(System.nanoTime()).nextInt(Math.toIntExact(TimeSpan.ELECTION_TIMEOUT_ADDITIONAL_RANGE))
            +
            TimeSpan.ELECTION_TIMEOUT_BASE;

    @Override
    public void run() {
        try {
            synchronized (this) {
                super.wait(electionTimeout);
                log.info("not collecting enough votes after {} ms", electionTimeout);
                if (isSplitVote()) {
                    new ReElectionInitiateProcedure().start();
                }
            }
        } catch (InterruptedException e) {
            log.info("restart to solve split vote OR is LEADER or has other LEADER, exit without any further action");
        }
    }

    /* if it remains CANDIDATE, then there'a a split vote */
    private boolean isSplitVote() {
        return NodeStatus.role().equals(Role.CANDIDATE);
    }
}
