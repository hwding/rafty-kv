package com.amastigote.raftymicrocluster.handler.msg;

import com.amastigote.raftymicrocluster.NodeStatus;
import com.amastigote.raftymicrocluster.handler.GeneralInboundDatagramHandler;
import com.amastigote.raftymicrocluster.protocol.Role;
import lombok.extern.slf4j.Slf4j;

/**
 * @author: hwding
 * @date: 2018/12/3
 */
@SuppressWarnings("JavaDoc")
@Slf4j(topic = "[HANDLER HEARTBEAT MSG]")
public class HeartbeatMsgDispatcher {
    public static void dispatch(GeneralInboundDatagramHandler.HeartbeatWatchdogResetInvoker heartbeatWatchdogResetInvoker) {
        if (NodeStatus.role().equals(Role.LEADER)) {
            log.warn("other leader's heartbeat recv, step down");

            synchronized (NodeStatus.class) {
                NodeStatus.setRoleTo(Role.FOLLOWER);
                NodeStatus.heartbeatThread().interrupt();

                heartbeatWatchdogResetInvoker.apply(false);
            }
            return;
        }

        if (NodeStatus.role().equals(Role.CANDIDATE)) {

            /* give up election procedure */
            synchronized (NodeStatus.class) {
                NodeStatus.setRoleTo(Role.FOLLOWER);
            }
            if (NodeStatus.voteCntTimeoutDetectThread().isAlive()) {
                NodeStatus.voteCntTimeoutDetectThread().interrupt();
            }

            heartbeatWatchdogResetInvoker.apply(false);

            return;
        }

        /* Role.FOLLOWER */
        heartbeatWatchdogResetInvoker.apply(true);
    }
}