package com.amastigote.raftymicrocluster.handler.msg;

import com.amastigote.raftymicrocluster.NodeStatus;
import com.amastigote.raftymicrocluster.handler.GeneralInboundDatagramHandler;
import com.amastigote.raftymicrocluster.protocol.Role;

/**
 * @author: hwding
 * @date: 2018/12/3
 */
@SuppressWarnings("JavaDoc")
public class HeartbeatMsgDispatcher {
    public static void dispatch(GeneralInboundDatagramHandler.HeartbeatWatchdogResetInvoker heartbeatWatchdogResetInvoker) {
        if (NodeStatus.role().equals(Role.LEADER)) {

            /* step down */
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