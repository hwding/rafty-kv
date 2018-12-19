package com.amastigote.raftymicrocluster.handler.msg;

import com.amastigote.raftymicrocluster.NodeStatus;
import com.amastigote.raftymicrocluster.handler.GeneralInboundDatagramHandler;
import com.amastigote.raftymicrocluster.procedure.ReplyAppendEntryResultProcedure;
import com.amastigote.raftymicrocluster.protocol.GeneralMsg;
import com.amastigote.raftymicrocluster.protocol.MsgType;
import com.amastigote.raftymicrocluster.protocol.Role;
import lombok.extern.slf4j.Slf4j;

/**
 * @author: hwding
 * @date: 2018/12/3
 */
@SuppressWarnings("JavaDoc")
@Slf4j(topic = "[HANDLER HEARTBEAT MSG]")
public class HeartbeatMsgDispatcher {
    public static void dispatch(
            final GeneralMsg msg,
            final GeneralInboundDatagramHandler.HeartbeatWatchdogResetInvoker heartbeatWatchdogResetInvoker
    ) {
        log.info("HeartbeatMsgDispatcher dispatching...");

        if (MsgType.RpcAnalogType.REQ.equals(msg.getRpcAnalogType())) {
            if (NodeStatus.role().equals(Role.LEADER)) {
                log.warn("other leader's heartbeat recv, step down");

                synchronized (NodeStatus.class) {
                    NodeStatus.transferRoleTo(Role.FOLLOWER);
                    NodeStatus.heartbeatThread().interrupt();

                    heartbeatWatchdogResetInvoker.apply(false);
                }
                return;
            }

            if (NodeStatus.role().equals(Role.CANDIDATE)) {
                /* give up election procedure */
                synchronized (NodeStatus.class) {
                    NodeStatus.transferRoleTo(Role.FOLLOWER);

                    if (NodeStatus.voteResWatchdogThread().isAlive()) {
                        NodeStatus.voteResWatchdogThread().interrupt();
                    }

                    heartbeatWatchdogResetInvoker.apply(false);
                }
                return;
            }

            /* Role.FOLLOWER */
            heartbeatWatchdogResetInvoker.apply(true);

            NodeStatus.FollowerAppendEntryResultContext context = NodeStatus.appendEntry(
                    msg.getEntries(), msg.getPrevLogIdx(), msg.getPrevLogTerm(), msg.getLeaderCommittedIdx()
            );
            if (context.isNeedRespond()) {
                context.setResToPort(msg.getResponseToPort());
                new Thread(new ReplyAppendEntryResultProcedure(context)).start();
            } else {
                log.info("no need to respond to current heartbeat");
            }

            return;
        }

        /* RES of AppendLogRPC */
        if (MsgType.RpcAnalogType.RES.equals(msg.getRpcAnalogType())) {
            NodeStatus.updateFollowerEntriesState(msg.getResponseToPort(), msg.getLastReplicatedLogIdx());
        }
    }
}