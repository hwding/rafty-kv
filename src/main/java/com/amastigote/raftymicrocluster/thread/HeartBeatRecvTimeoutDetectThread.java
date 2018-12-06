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
@Slf4j(topic = "[HEARTBEAT TIMEOUT DETC THREAD]")
public class HeartBeatRecvTimeoutDetectThread extends Thread {
    private final static long heartBeatTimeout =
            new Random(System.nanoTime()).nextInt(Math.toIntExact(TimeSpan.HEARTBEAT_TIMEOUT_ADDITIONAL_RANGE))
                    +
                    TimeSpan.HEARTBEAT_TIMEOUT_BASE;

    @Override
    public void run() {
        while (!this.isInterrupted()) {
            try {
                synchronized (this) {
                    super.wait(heartBeatTimeout);
                }

                log.warn("HB not recv in {} ms", heartBeatTimeout);
                if (NodeStatus.role().equals(Role.CANDIDATE)) {
                    continue;
                }
                if (NodeStatus.role().equals(Role.LEADER)) {
                    synchronized (NodeStatus.class) {
                        NodeStatus.heartbeatThread().interrupt();
                        NodeStatus.setRoleTo(Role.FOLLOWER);
                        NodeStatus.heartbeatRecvTimeoutDetectThread().start();
                    }
                }
                new ReElectionInitiateProcedure().start();
            } catch (InterruptedException e) {
                synchronized (NodeStatus.class) {

                    /* if interrupted due to role changing, stop thread */
                    if (!NodeStatus.role().equals(Role.FOLLOWER)) {
                        log.info("HB recv in NON-FOLLOWER state, exit");
                        break;
                    }

                    /* else continuing timing */
                    log.info("HB recv, reset timer");
                }
            }
        }
    }
}
