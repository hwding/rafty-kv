package com.amastigote.raftymicrocluster.procedure;

import com.amastigote.raftymicrocluster.NodeStatus;
import com.amastigote.raftymicrocluster.protocol.Role;
import lombok.extern.slf4j.Slf4j;

/**
 * @author: hwding
 * @date: 2018/11/29
 */
@SuppressWarnings("JavaDoc")
@Slf4j(topic = "[RE-ELECTION INIT PROC]")
public class ReElectionInitiateProcedure extends Thread {

    @Override
    public void run() {
        log.info("ReElectionInitiateProcedure start...");

        long newTerm;

        /* transfer state */
        synchronized (NodeStatus.class) {
            newTerm = NodeStatus.incrCurrentTerm();
            NodeStatus.transferRoleTo(Role.CANDIDATE);

            /* candidate should first vote for itself */
            NodeStatus.rstVoteCnt(1);
            NodeStatus.rstVotedFor();
        }
        log.info("term increased to " + newTerm);

        /* reset concerning timers */
        synchronized (NodeStatus.class) {
            NodeStatus.rstVoteResWatchdogThread(true);
            NodeStatus.rstHeartBeatWatchdogThread(false);
        }

        new RequestVoteProcedure().start();

        log.info("ReElectionInitiateProcedure end...");
    }
}
