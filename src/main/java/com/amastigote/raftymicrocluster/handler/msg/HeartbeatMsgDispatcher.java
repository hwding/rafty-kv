package com.amastigote.raftymicrocluster.handler.msg;

import com.amastigote.raftymicrocluster.NodeStatus;
import com.amastigote.raftymicrocluster.handler.GeneralInboundDatagramHandler;
import com.amastigote.raftymicrocluster.protocol.GeneralMsg;
import com.amastigote.raftymicrocluster.protocol.Role;

/**
 * @author: hwding
 * @date: 2018/12/3
 */
@SuppressWarnings("JavaDoc")
public class HeartbeatMsgDispatcher {
    public static void dispatch(GeneralMsg msg, GeneralInboundDatagramHandler.HeartbeatWatchdogResetInvoker heartbeatWatchdogResetInvoker) {
        if (NodeStatus.role().equals(Role.LEADER) && msg.getTerm() > NodeStatus.currentTerm()) {
            NodeStatus.reInitTerm(msg.getTerm());

            /* step down */
            synchronized (NodeStatus.class) {
                NodeStatus.setRoleTo(Role.FOLLOWER);
                NodeStatus.heartbeatThread().interrupt();

                heartbeatWatchdogResetInvoker.apply(false);
            }
            return;
        }

        heartbeatWatchdogResetInvoker.apply(true);
    }
}