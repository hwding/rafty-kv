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
@Slf4j(topic = "[HEARTBEAT TIMEOUT DETC THREAD]")
public class HeartBeatRecvTimeoutDetectThread extends Thread {
    private long heartBeatTimeout = new Random(System.nanoTime()).nextInt(2000) + Timeout.HEARTBEAT_TIMEOUT_BASE;

    @Override
    public void run() {
        while (!super.isInterrupted()) {
            try {
                synchronized (this) {
                    super.wait(heartBeatTimeout);
                }

                log.warn("HB not recv in " + heartBeatTimeout + "ms");
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
                        break;
                    }

                    /* else continuing timing */
                    log.info("HB recv, reset timer");
                }
            }
        }
    }
}
