package com.amastigote.raftykv.handler.msg;

import com.amastigote.raftykv.NodeState;
import com.amastigote.raftykv.handler.GeneralInboundDatagramHandler;
import com.amastigote.raftykv.procedure.VoteForCandidateProcedure;
import com.amastigote.raftykv.protocol.GeneralMsg;
import com.amastigote.raftykv.protocol.MsgType;
import com.amastigote.raftykv.protocol.Role;
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
            int compareToRecvTerm,
            GeneralInboundDatagramHandler.HeartbeatWatchdogResetInvoker heartbeatWatchdogResetInvoker
    ) {
        log.info("ElectMsgDispatcher dispatching...");
        MsgType.RpcAnalogType rpcAnalogType = msg.getRpcAnalogType();

        if (rpcAnalogType.equals(MsgType.RpcAnalogType.REQ)) {
            NodeState.heartBeatWatchdogThread().interrupt();
            if (NodeState.role().equals(Role.LEADER)) {

                /* step down and vote for this candidate */
                synchronized (NodeState.class) {
                    NodeState.transferRoleTo(Role.FOLLOWER);
                    NodeState.heartbeatThread().interrupt();

                    heartbeatWatchdogResetInvoker.apply(false);

                    log.info("step down from LEADER and vote for newer term candidate");
                }
                new VoteForCandidateProcedure(
                        msg.getResponseToPort(),
                        msg.getTerm(),
                        msg.getLastLogIdx(),
                        msg.getLastLogTerm()
                ).start();
                return;
            }

            if (NodeState.role().equals(Role.CANDIDATE)) {
                if (compareToRecvTerm == -1) {

                    /* step down and vote for this newer candidate */
                    synchronized (NodeState.class) {
                        NodeState.transferRoleTo(Role.FOLLOWER);

                        /* end campaign waiting in advance */
                        NodeState.voteResWatchdogThread().interrupt();

                        heartbeatWatchdogResetInvoker.apply(false);
                    }
                    new VoteForCandidateProcedure(
                            msg.getResponseToPort(),
                            msg.getTerm(),
                            msg.getLastLogIdx(),
                            msg.getLastLogTerm()
                    ).start();

                    log.info("step down from CANDIDATE and vote for newer term candidate");
                    return;
                }

                log.info("competitor's same term vote req, do nothing");
                return;
            }

            if (NodeState.role().equals(Role.FOLLOWER) && (compareToRecvTerm <= 0)) {

                /* the follower should remain its state as long as it receives valid RPCs from leader OR **candidate** */
                heartbeatWatchdogResetInvoker.apply(true);

                new VoteForCandidateProcedure(
                        msg.getResponseToPort(),
                        msg.getTerm(),
                        msg.getLastLogIdx(),
                        msg.getLastLogTerm()
                ).start();
            }
            return;
        }

        if (rpcAnalogType.equals(MsgType.RpcAnalogType.RES)) {
            if (NodeState.role().equals(Role.CANDIDATE)) {
                int voteCnt = NodeState.incrVoteCnt();
                if (voteCnt >= NodeState.majorityNodeCnt()) {
                    log.info("nice, i'm leader now with voteCnt {}", voteCnt);
                    synchronized (NodeState.class) {
                        NodeState.transferRoleTo(Role.LEADER);

                        NodeState.rstHeartbeatThread(true);

                        NodeState.heartBeatWatchdogThread().interrupt();
                        NodeState.voteResWatchdogThread().interrupt();
                    }
                }
                return;
            }

            log.info("the candidate may stepped down or already become leader, ignore vote RES");
        }
    }
}
