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
@Slf4j(topic = "[HEARTBEAT WATCHDOG THREAD]")
public class HeartBeatWatchdogThread extends Thread {
    private final static long heartBeatTimeout =
            new Random(System.nanoTime()).nextInt(Math.toIntExact(TimeSpan.HEARTBEAT_TIMEOUT_ADDITIONAL_RANGE))
                    +
                    TimeSpan.HEARTBEAT_TIMEOUT_BASE;

    @Override
    public void run() {
        log.info("heartbeat watchdog thread start running...");
        while (!this.isInterrupted()) {
            try {
                synchronized (this) {
                    super.wait(heartBeatTimeout);
                }

                /* if no longer follower after awake */
                if (!NodeStatus.role().equals(Role.FOLLOWER)) {
                    log.info("no longer FOLLOWER after awake, exit");
                    break;
                }

                /* else still FOLLOWER */
                log.warn("heartbeat not recv in {} ms", heartBeatTimeout);
                new ReElectionInitiateProcedure().start();
            } catch (InterruptedException e) {
                synchronized (NodeStatus.class) {

                    /* if interrupted due to role changing, stop thread */
                    if (!NodeStatus.role().equals(Role.FOLLOWER)) {
                        log.info("heartbeat watchdog interrupted in NON-FOLLOWER state, exit");
                        break;
                    }

                    /* else continuing timing */
                    log.info("heartbeat recv, reset timer");
                }
            }
        }
    }
}
