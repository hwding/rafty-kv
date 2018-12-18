package com.amastigote.raftymicrocluster.handler.msg;

import com.amastigote.raftymicrocluster.NodeStatus;
import com.amastigote.raftymicrocluster.handler.GeneralInboundDatagramHandler;
import com.amastigote.raftymicrocluster.procedure.VoteForCandidateProcedure;
import com.amastigote.raftymicrocluster.protocol.GeneralMsg;
import com.amastigote.raftymicrocluster.protocol.MsgType;
import com.amastigote.raftymicrocluster.protocol.Role;
import lombok.extern.slf4j.Slf4j;

/**
 * @author: hwding
 * @date: 2018/12/3
 */
@SuppressWarnings("JavaDoc")
@Slf4j(topic = "[DISPATCHER ELECT-MSG]")
public final class ElectMsgDispatcher {

    public static void dispatch(
            GeneralMsg msg,
            boolean newerTerm,
            GeneralInboundDatagramHandler.HeartbeatWatchdogResetInvoker heartbeatWatchdogResetInvoker
    ) {
        log.info("ElectMsgDispatcher dispatching...");
        MsgType.RpcAnalogType rpcAnalogType = msg.getRpcAnalogType();

        if (rpcAnalogType.equals(MsgType.RpcAnalogType.REQ)) {
            NodeStatus.heartBeatWatchdogThread().interrupt();
            if (NodeStatus.role().equals(Role.LEADER)) {

                /* step down and vote for this candidate */
                synchronized (NodeStatus.class) {
                    NodeStatus.setRoleTo(Role.FOLLOWER);
                    NodeStatus.heartbeatThread().interrupt();

                    heartbeatWatchdogResetInvoker.apply(false);

                    log.info("step down from LEADER and vote for newer term candidate");
                }
                new VoteForCandidateProcedure(msg.getResponseToPort(), msg.getTerm()).start();
                return;
            }

            if (NodeStatus.role().equals(Role.CANDIDATE)) {
                if (newerTerm) {

                    /* step down and vote for this newer candidate */
                    synchronized (NodeStatus.class) {
                        NodeStatus.setRoleTo(Role.FOLLOWER);

                        /* end campaign waiting in advance */
                        NodeStatus.voteResWatchdogThread().interrupt();

                        heartbeatWatchdogResetInvoker.apply(false);
                    }
                    new VoteForCandidateProcedure(msg.getResponseToPort(), msg.getTerm()).start();

                    log.info("step down from CANDIDATE and vote for newer term candidate");
                    return;
                }

                log.info("competitor's same term vote req, do nothing");
                return;
            }

            if (NodeStatus.role().equals(Role.FOLLOWER) && newerTerm) {

                /* the follower should remain its state as long as it receives valid RPCs from leader OR **candidate** */
                heartbeatWatchdogResetInvoker.apply(true);

                new VoteForCandidateProcedure(msg.getResponseToPort(), msg.getTerm()).start();
            }
            return;
        }

        if (rpcAnalogType.equals(MsgType.RpcAnalogType.RES)) {
            if (NodeStatus.role().equals(Role.CANDIDATE)) {
                int voteCnt = NodeStatus.incrVoteCnt();
                if (voteCnt >= NodeStatus.majorityNodeCnt()) {
                    log.info("nice, i'm leader now with voteCnt {}", voteCnt);
                    synchronized (NodeStatus.class) {
                        NodeStatus.setRoleTo(Role.LEADER);

                        NodeStatus.rstHeartbeatThread(true);

                        NodeStatus.heartBeatWatchdogThread().interrupt();
                        NodeStatus.voteResWatchdogThread().interrupt();
                    }
                }
                return;
            }

            log.info("the candidate may stepped down or already become leader, ignore vote RES");
        }
    }
}
