package com.amastigote.raftykv.procedure;

import com.amastigote.raftykv.NodeState;
import com.amastigote.raftykv.protocol.Role;
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
        synchronized (NodeState.class) {
            newTerm = NodeState.incrCurrentTerm();
            NodeState.transferRoleTo(Role.CANDIDATE);

            /* candidate should first vote for itself */
            NodeState.rstVotedFor();
        }
        log.info("term increased to " + newTerm);

        /* reset concerning timers */
        synchronized (NodeState.class) {
            NodeState.rstVoteResWatchdogThread(true);
            NodeState.rstHeartBeatWatchdogThread(false);
        }

        new RequestVoteProcedure().start();

        log.info("ReElectionInitiateProcedure end...");
    }
}
