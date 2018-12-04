package com.amastigote.raftymicrocluster.procedure;

import com.amastigote.raftymicrocluster.NodeStatus;
import com.amastigote.raftymicrocluster.protocol.Role;
import com.amastigote.raftymicrocluster.thread.VoteCntTimeoutDetectThread;
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
        log.info("! leader reelection initiate...");

        long newTerm;

        /* transfer state */
        synchronized (NodeStatus.class) {
            newTerm = NodeStatus.incrTerm();
            NodeStatus.setRoleTo(Role.CANDIDATE);

            /* candidate should first vote for itself */
            NodeStatus.resetVoteCnt(1);
            NodeStatus.resetVotedFor();
        }
        log.info("term increased to " + newTerm);

        new RequestVoteProcedure().start();

        /* reset concerning timers */
        synchronized (NodeStatus.class) {
            if (NodeStatus.voteCntTimeoutDetectThread().isAlive()) {
                NodeStatus.voteCntTimeoutDetectThread().interrupt();
            }

            NodeStatus.setVoteCntTimeoutDetectThread(new VoteCntTimeoutDetectThread());
            NodeStatus.voteCntTimeoutDetectThread().start();

            if (NodeStatus.heartbeatRecvTimeoutDetectThread().isAlive()) {
                NodeStatus.heartbeatRecvTimeoutDetectThread().interrupt();
            }
        }
    }
}
