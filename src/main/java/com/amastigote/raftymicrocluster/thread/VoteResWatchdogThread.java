package com.amastigote.raftymicrocluster.thread;

import com.amastigote.raftymicrocluster.NodeState;
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
@Slf4j(topic = "[VOTE RES WATCHDOG THREAD]")
public class VoteResWatchdogThread extends Thread {
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
            log.info("is LEADER OR has other LEADER OR restart to solve split vote, exit without any further action");
        }
    }

    /* if it remains CANDIDATE, then there'a a split vote */
    private boolean isSplitVote() {
        return NodeState.role().equals(Role.CANDIDATE);
    }
}
